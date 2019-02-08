'use strict';

const assert = require('assert');
const path = require('path');
const ssri = require('ssri');
const stream = require('stream');
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

const pipeline = promisify(stream.pipeline);

const INTEGRITY_ALGO = 'sha512';

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
        const initState = require(path.resolve(cachePath, STATE_FILE_NAME));
        for (const [k, v] of initState) {
          super.set(k, v);
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
    try {
      await unlink(file);
    } catch (err) {}
  }

  async persist() {
    if (!this.persistent) return;
    await writeFile(
      path.resolve(this.cachePath, STATE_FILE_NAME),
      JSON.stringify([...this]),
      'utf8',
    );
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
    try {
      // write data to disk
      await writeFile(filename, data, { flag: 'wx' });
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      // check that existing data is still valid?
      try {
        await ssri.checkStream(createReadStream(filename), integrity);
        super.set(key, entry);
        return entry;
      } catch (e) {
        if (e.code !== 'EINTEGRITY') throw err; // some other error happening
        // overwrite invalid data
        await writeFile(filename, data);
      }
    }

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
    if (!entry) return res;
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
    try {
      // check if there is a valid file in place
      await ssri.checkStream(createReadStream(filename), integrity.toString());
      // just remove temp file
      await this._safeUnlink(tmpFilename);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      // move temp file to new location
      await rename(tmpFilename, filename);
    }
    super.set(key, entry);
    await this.persist();
    return entry;
  }

  /**
   *
   * @param {{ integrity?: import('ssri').Integrity, size?: number }} calcObj
   * @returns {stream.Transform}
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
   * @returns {stream.Writable}
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
    return new stream.Writable({
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
   * @returns {stream.Writable}
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
    return new stream.Transform({
      transform(chunk, enc, cb) {
        // this.push(chunk, enc);
        calcStream.write(chunk, enc, err => {
          if (err) throw err;
          tmpFile.stream.write(chunk, enc, err => {
            cb(err, chunk);
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
   * @param {stream.Readable} stream
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
   * @returns {stream.Readable}
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
