'use strict';

const path = require('path');
const ssri = require('ssri');
const stream = require('stream');
const {
  mkdirSync,
  createReadStream,
  createWriteStream,
  constants: { R_OK },
} = require('fs');
const {
  unlink,
  writeFile,
  readFile,
  mkdir,
  access,
  rename,
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
      time: Date,
      metadata?: any,
    }} CacheEntity
   */

/**
 * @extends {Map<string, CacheEntity>}
 */
class DestCache extends Map {
  /**
   *
   * @param {string} cachePath - directory where cache will be located
   */
  constructor(cachePath) {
    super();
    // ensure directory exists
    try {
      mkdirSync(cachePath, { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
    }
    this.cachePath = cachePath;
  }

  /**
   *
   * @param {string} key
   * @param {string | Buffer} data
   * @returns {Promise.<CacheEntity>}
   */
  async set(key, data, metadata = {}) {
    const integrity = ssri.fromData(data, { algorithms: [INTEGRITY_ALGO] });
    // store to disk
    const filename = path.join(this.cachePath, integrity.hexDigest());
    /** @type {CacheEntity} */
    const entry = {
      path: filename,
      size: data.length,
      integrity: integrity.toString(),
      time: new Date(),
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
    return entry;
  }

  /**
   *
   * @param {string} key
   */
  async delete(key) {
    // get current integrity
    const entry = super.get(key);
    const res = super.delete(key);
    if (!entry) return res;

    // find any other keys referring to this content
    for (const { integrity } of this.values()) {
      if (integrity === entry.integrity) return res;
    }
    // if there is no other keys referring this content - remove it
    await unlink(entry.path);
    return res;
  }

  /**
   *
   * @param {string} key
   * @returns {Promise.<Buffer>}
   */
  async get(key) {
    if (!super.has(key)) return undefined;
    const entry = super.get(key);
    const data = await readFile(entry.path);

    if (!ssri.checkData(data, entry.integrity)) {
      super.delete(key);
      await unlink(entry.path);
      const err = new Error(`Invalid integrity for key ${key}`);
      err.code = 'EINTEGRITY';
      throw err;
    }

    return data;
  }

  /**
   *
   * @param {string} key
   * @returns {false | CacheEntity}
   */
  has(key) {
    if (!super.has(key)) return false;
    return super.get(key);
  }

  /**
   *
   * @param {string} key
   * @param {stream.Readable} stream
   * @param {*} metadata
   */
  async setStream(key, stream, metadata = {}) {
    // stream to a temporary file
    const tempDirectory = path.join(this.cachePath, 'tmp');
    // ensure directory exists
    try {
      await mkdir(tempDirectory, { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
    }
    const tmpFilename = path.join(
      tempDirectory,
      randomBytes(100).toString('hex'),
    );

    // streaming
    /** @type {import('ssri').Integrity}*/
    let integrity;
    let size;
    await pipeline(
      stream,
      ssri
        .integrityStream({
          algorithms: [INTEGRITY_ALGO],
        })
        .on('integrity', s => {
          integrity = s;
        })
        .on('size', s => {
          size = s;
        }),
      createWriteStream(tmpFilename, {
        flags: 'wx',
      }),
    );

    // check if that integrity file does not exists yet, then move it to destination
    const filename = path.join(this.cachePath, integrity.hexDigest());
    /** @type {CacheEntity} */
    const entry = {
      path: filename,
      size,
      integrity: integrity.toString(),
      time: new Date(),
      metadata,
    };
    try {
      await access(filename, R_OK);
      // just remove temp file
      await unlink(tmpFilename);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      // move temp file to new location
      await rename(tmpFilename, filename);
    }
    super.set(key, entry);
    return entry;
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
      .once('error', err => {
        // clean file on integrity or size error
        if (err.code === 'EINTEGRITY' || err.code === 'EBADSIZE') {
          this.delete(key);
        }
      });
  }
}

module.exports = DestCache;
