/* eslint-disable max-depth */
/* eslint-disable sonarjs/no-identical-functions */
/* eslint-disable no-sync */
'use strict';

const assert = require('assert');
const path = require('path');
const {
  mkdirSync,
  statSync,
  createReadStream,
  createWriteStream,
} = require('fs');
const {
  unlink,
  writeFile,
  readFile,
  rename,
  mkdir,
  stat,
} = require('fs').promises;
const { promisify } = require('util');
const { randomBytes } = require('crypto');
const { Writable, Transform } = require('stream');
const pipeline = promisify(require('stream').pipeline);

const ssri = require('ssri');

const INTEGRITY_ALGO = 'md5';

/**
   * @typedef {{
      integrity: string,
      path: string,
      size: number,
      time: number,
      metadata?: any,
    }} CacheEntity
   */

const STATE_FILE_NAME = '.destr-cache.json';

class IntegrityError extends Error {
  /**
   *
   * @param {string} msg
   */
  constructor(msg) {
    super(msg);
    this.code = 'EINTEGRITY';
    this.name = 'IntegrityError';
  }
}

/**
 * @extends {Map<string, CacheEntity>}
 */
class DestCache extends Map {
  /**
   *
   * @param {string} cachePath - directory where cache will be located, it will be created synchronously on constructor call
   * @param {boolean} [persistent] - whether cache should save / restore it's state to the disk
   */
  constructor(cachePath, persistent = false) {
    super();
    if (persistent) {
      // reading previous state
      try {
        /** @type {[string, CacheEntity][]} */
        const initState = require(path.resolve(cachePath, STATE_FILE_NAME));
        for (const [k, v] of initState) {
          // we will check at least existence and size
          try {
            const st = statSync(v.path);
            if (!st.isFile()) {
              console.error(
                'Not restoring key %s because %s is not a file',
                k,
                v.path,
              );
              continue;
            }
            if (st.size !== v.size) {
              console.error(
                'Not restoring key %s due to %s size difference: %d != %d',
                k,
                v.path,
                st.size,
                v.size,
              );
            }
            // if creation time is more than 1 seconds different from our time
            if (st.ctimeMs - v.time > 1000) {
              console.log(
                'Not restoring key %s due to %s time difference: %d',
                k,
                v.path,
                st.ctimeMs - v.time,
              );
              continue;
            }
            super.set(k, v);
          } catch (err) {
            console.error(
              'Not restoring cache key %s due to file %s access failure',
              k,
              v.path,
            );
          }
        }
        // eslint-disable-next-line no-empty
      } catch (err) {}
    }
    this.cachePath = cachePath;
    this.persistent = persistent;
  }

  /**
   * @private
   */
  async _ensureCacheDirectory() {
    try {
      await mkdir(this.cachePath, { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      // maker sure it's really a directory
      const st = await stat(this.cachePath);
      assert.ok(
        st.isDirectory(),
        `Unable to create ${this.cachePath} due to a file with the same name`,
      );
    }
  }

  /**
   * @private
   */
  get tempDirectory() {
    return path.join(this.cachePath, 'tmp');
  }

  /**
   * @private
   */
  _ensureTempDirectorySync() {
    try {
      mkdirSync(this.tempDirectory, { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      assert.ok(
        statSync(this.tempDirectory).isDirectory(),
        `Unable to create ${
          this.tempDirectory
        } due to a file with the same name`,
      );
    }
  }

  /**
   * @private
   * @param {string} file
   */
  async _safeUnlink(file) {
    // will try 3 times
    for (let i = 0; i < 3; i++) {
      try {
        await unlink(file);
      } catch (err) {
        // if not exist - return
        if (err.code === 'ENOENT') return;
        if (
          err.code === 'EMFILE' ||
          (err.code === 'EPERM' && process.platform === 'win32')
        ) {
          await new Promise(resolve => setTimeout(resolve, i * 1000));
          continue;
        }
        // give up
        break;
      }
    }
  }

  async persist() {
    if (!this.persistent) return;
    // retry to times
    for (let i = 0; i < 3; i++) {
      try {
        await writeFile(
          path.resolve(this.cachePath, STATE_FILE_NAME),
          JSON.stringify([...this]),
          'utf8',
        );
        break;
      } catch (err) {
        if (
          err.code === 'EMFILE' ||
          (process.platform === 'win32' && err.code === 'EPERM')
        ) {
          // sleep and continue
          await new Promise(resolve => setTimeout(resolve, i * 1000));
          continue;
        }
        break;
      }
    }
  }

  /**
   *
   * @param {string} key
   * @param {string | Buffer} data
   * @returns {Promise.<CacheEntity>}
   */
  // @ts-ignore
  async set(key, data, metadata = {}) {
    await this._ensureCacheDirectory();

    const integrity = ssri.fromData(data, { algorithms: [INTEGRITY_ALGO] });
    // store to disk
    const filename = path.join(this.cachePath, integrity.hexDigest());
    /** @type {CacheEntity} */
    const entry = {
      path: filename,
      size: data.length,
      integrity: integrity.toString(),
      time: Date.now(),
      metadata,
    };

    // will try 6 times
    let lastError = '';
    let writeFlag = 'wx'; // fail if file exists
    let i = 0;
    for (i; i < 6; i++)
      try {
        // write data to disk
        await writeFile(filename, data, { flag: writeFlag });
        lastError = '';
        break;
      } catch (err) {
        lastError = err.code;
        // check that existing data is still valid?
        if (lastError === 'EEXIST')
          try {
            await ssri.checkStream(createReadStream(filename), integrity);
            lastError = '';
            break;
          } catch (e) {
            lastError = e.code;
            // overwrite invalid data
            writeFlag = 'w'; // open for writing
          }
        else if (
          lastError === 'EMFILE' ||
          (process.platform === 'win32' && lastError === 'EPERM')
        ) {
          writeFlag = 'w';
          await new Promise(resolve => setTimeout(resolve, i * 1000));
        }
      }

    if (lastError)
      throw new Error(
        `Failed to write key ${key} to ${filename}, error code: ${lastError}, retried ${i} times`,
      );
    super.set(key, entry);
    await this.persist();
    return entry;
  }

  /**
   *
   * @param {string} key
   */
  // @ts-ignore
  async delete(key) {
    // get current integrity
    const entry = super.get(key);
    const res = super.delete(key);
    if (!res) return res;
    await this._ensureCacheDirectory();
    await this.persist();

    // find any other keys referring to this content
    for (const { integrity } of this.values()) {
      if (integrity === entry.integrity) return res;
    }
    // if there is no other keys referring this content - remove it
    await this._safeUnlink(entry.path);
    return res;
  }

  /**
   *
   * @param {string} key
   * @returns {Promise.<Buffer | undefined>}
   */
  // @ts-ignore
  async get(key) {
    if (!super.has(key)) return undefined;
    const entry = super.get(key);
    const data = await readFile(entry.path);

    if (!ssri.checkData(data, entry.integrity)) {
      super.delete(key);
      await this._safeUnlink(entry.path);
      throw new IntegrityError(`Invalid integrity for key ${key}`);
    }

    return data;
  }

  /**
   *
   * @param {string} key
   * @returns {false | CacheEntity}
   */
  // @ts-ignore
  has(key) {
    if (!super.has(key)) return false;
    return super.get(key);
  }

  /**
   * @private
   */
  _getTmpFileWriteStream() {
    this._ensureTempDirectorySync();

    // stream to a temporary file
    const tmpFilename = path.join(
      this.tempDirectory,
      randomBytes(20).toString('hex'),
    );
    const ws = createWriteStream(tmpFilename, {
      flags: 'wx',
    });
    return { path: tmpFilename, stream: ws };
  }

  /**
   * @private
   * @param {string} tmpFilename
   * @param {string} key
   * @param {{ integrity?: import('ssri').Integrity, size?: number }} calcObj
   * @param {*} [metadata]
   * @returns {Promise.<CacheEntity>}
   */

  async _moveToCacheLocation(
    tmpFilename,
    key,
    { integrity, size },
    metadata = {},
  ) {
    // check if that integrity file does not exists yet, then move it to destination
    const filename = path.join(this.cachePath, integrity.hexDigest());
    /** @type {CacheEntity} */
    const entry = {
      path: filename,
      size,
      integrity: integrity.toString(),
      time: Date.now(),
      metadata,
    };
    // try 3 times
    for (let i = 0; i < 3; i++) {
      try {
        // check if there is a valid file in place
        await ssri.checkStream(
          createReadStream(filename),
          integrity.toString(),
        );
        // just remove temp file
        await this._safeUnlink(tmpFilename);
        break;
      } catch (err) {
        if (err.code === 'ENOENT') break;
        if (process.platform === 'win32' && err.code === 'EPERM') {
          await new Promise(resolve => setTimeout(resolve, i * 1000));
          continue;
        }
        throw err;
      }
    }
    // if temp file still exist that means we should move it to new location
    try {
      // move temp file to new location
      await rename(tmpFilename, filename);
    } catch (err) {
      if (err.code !== 'ENOENT') {
        if (process.platform === 'win32') {
          if (err.code !== 'EPERM') throw err;
        } else throw err;
      }
    }
    super.set(key, entry);
    await this.persist();
    return entry;
  }

  /**
   *
   * @param {{ integrity?: import('ssri').Integrity, size?: number }} calcObj
   * @returns {import('stream').Transform}
   * @private
   */
  _getSsriCalcStream(calcObj) {
    return ssri
      .integrityStream({
        single: true,
        algorithms: [INTEGRITY_ALGO],
      })
      .once('integrity', s => {
        calcObj.integrity = s;
      })
      .once('size', s => {
        calcObj.size = s;
      });
  }

  /**
   *
   * @param {string} key
   * @param {*} [metadata]
   * @returns {Writable}
   */
  getWriteStream(key, metadata = {}) {
    const tmpFile = this._getTmpFileWriteStream();
    const calcObj = {};
    const calcStream = this._getSsriCalcStream(calcObj);

    const mover = this._moveToCacheLocation.bind(
      this,
      tmpFile.path,
      key,
      calcObj,
      metadata,
    );
    return new Writable({
      write(chunk, enc, cb) {
        calcStream.write(chunk, enc, err => {
          if (err) throw err;
          tmpFile.stream.write(chunk, enc, cb);
        });
      },
      final(cb) {
        calcStream.once('integrity', () => {
          tmpFile.stream.end(async () => {
            await mover();
            cb();
          });
        });
        calcStream.emit('end');
      },
    });
  }

  /**
   *
   * @param {string} key
   * @param {*} [metadata]
   * @returns {Writable}
   */
  createCachingStream(key, metadata) {
    const tmpFile = this._getTmpFileWriteStream();
    const calcObj = {};
    const calcStream = this._getSsriCalcStream(calcObj);

    const mover = this._moveToCacheLocation.bind(
      this,
      tmpFile.path,
      key,
      calcObj,
      metadata,
    );
    return new Transform({
      transform(chunk, enc, cb) {
        // this.push(chunk, enc);
        calcStream.write(chunk, enc, err => {
          if (err) throw err;
          tmpFile.stream.write(chunk, enc, error => {
            cb(error, chunk);
          });
        });
      },
      final(cb) {
        calcStream.once('integrity', () => {
          tmpFile.stream.end(async () => {
            await mover();
            cb();
          });
        });
        calcStream.emit('end');
      },
    });
  }

  /**
   *
   * @param {string} key
   * @param {import('stream').Readable} stream
   * @param {*} metadata
   */
  async setStream(key, stream, metadata = {}) {
    // streaming
    const calcObj = {};
    const tmpFile = this._getTmpFileWriteStream();
    await pipeline(stream, this._getSsriCalcStream(calcObj), tmpFile.stream);

    return this._moveToCacheLocation(tmpFile.path, key, calcObj, metadata);
  }

  /**
   *
   * @param {string} key
   * @returns {import('stream').Readable}
   */
  getStream(key) {
    const entry = super.get(key);
    if (!entry) {
      return null;
    }

    // return stream
    return createReadStream(entry.path)
      .pipe(
        ssri.integrityStream({ integrity: entry.integrity, size: entry.size }),
      )
      .once(
        'error',
        /**
         * @param {NodeJS.ErrnoException} err
         */
        async err => {
          // clean file on integrity or size error
          if (err.code === 'EINTEGRITY' || err.code === 'EBADSIZE') {
            await this.delete(key);
          }
        },
      );
  }
}

module.exports = DestCache;
