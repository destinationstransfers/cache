'use strict';

const crypto = require('crypto');
const figgyPudding = require('figgy-pudding');
const glob = require('fast-glob');
const path = require('path');
const ssri = require('ssri');
const { readFile, appendFile } = require('fs').promises;
const { Transform } = require('stream');

const contentPath = require('./content/path');
const hashToSegments = require('./util/hash-to-segments');
const { mkdirfix, chownr } = require('./util/fix-owner');

const indexV = require('../package.json')['cache-version'].index;

/**
 * @typedef {{
    key: string,
    integrity: string,
    path: string,
    size: number,
    time: number,
    metadata: any,
  }} FormattedEntry
 */

function _bucketEntries(data) {
  let entries = [];
  data.split('\n').forEach(entry => {
    if (!entry) {
      return;
    }
    const pieces = entry.split('\t');
    if (!pieces[1] || hashEntry(pieces[1]) !== pieces[0]) {
      // Hash is no good! Corruption or malice? Doesn't matter!
      // EJECT EJECT
      return;
    }
    let obj;
    try {
      obj = JSON.parse(pieces[1]);
    } catch (err) {
      // Entry is corrupted!
      return;
    }
    if (obj) {
      entries.push(obj);
    }
  });
  return entries;
}

/**
 *
 * @param {string} bucket
 * @returns {Promise.<FormattedEntry[]>}
 */
async function bucketEntries(bucket) {
  const data = await readFile(bucket, 'utf8');
  return _bucketEntries(data);
}

module.exports.NotFoundError = class NotFoundError extends Error {
  constructor(cache, key) {
    super(`No cache entry for \`${key}\` found in \`${cache}\``);
    this.code = 'ENOENT';
    this.cache = cache;
    this.key = key;
  }
};

const IndexOpts = figgyPudding({
  metadata: {},
  size: {},
  uid: {},
  gid: {},
});

/**
 *
 * @param {string} cache
 * @param {string} key
 * @param {string} integrity
 * @param {{ size?: number, metadata: any, uid?: number, gid?: number }} opts
 * @returns {Promise.<FormattedEntry>}
 */
async function insert(cache, key, integrity, opts) {
  opts = IndexOpts(opts);
  const bucket = bucketPath(cache, key);
  /** @type {Partial<FormattedEntry>} */
  const entry = {
    key,
    integrity: integrity && ssri.stringify(integrity),
    time: Date.now(),
    size: opts.size,
    metadata: opts.metadata,
  };
  await mkdirfix(path.dirname(bucket), opts.uid, opts.gid);
  const stringified = JSON.stringify(entry);
  await appendFile(bucket, `\n${hashEntry(stringified)}\t${stringified}`);
  try {
    await chownr(bucket, opts.uid, opts.gid);
  } catch (err) {
    if (err.code !== 'ENOENT') {
      throw err;
    }
  }
  return formatEntry(cache, entry);
}
module.exports.insert = insert;

/**
 *
 * @param {string} cache
 * @param {string} key
 * @returns {Promise.<FormattedEntry>}
 */
async function find(cache, key) {
  const bucket = bucketPath(cache, key);
  try {
    const entries = await bucketEntries(bucket);
    return entries.reduce((latest, next) => {
      if (next && next.key === key) {
        return formatEntry(cache, next);
      } else {
        return latest;
      }
    }, null);
  } catch (err) {
    if (err.code === 'ENOENT') {
      return null;
    } else {
      throw err;
    }
  }
}
module.exports.find = find;

/**
 *
 * @param {string} cache
 * @param {string} key
 * @param {*} opts
 */
function del(cache, key, opts) {
  return insert(cache, key, null, opts);
}
module.exports.delete = del;

class BucketStream extends Transform {
  constructor(cache) {
    super({ objectMode: true });
    this.cache = cache;
  }

  async _transform(chunk, enc, cb) {
    try {
      const be = await bucketEntries(chunk);

      // filters unique entries this way
      const keyToEntry = be.reduce((acc, entry) => {
        acc.set(entry.key, entry);
        return acc;
      }, new Map());

      for (const entry of keyToEntry.values()) {
        const formatted = formatEntry(this.cache, entry);
        if (formatted) this.push(formatted);
      }
      cb(null);
    } catch (err) {
      if (err.code !== 'ENOENT') cb(err);
    }
  }
}

/**
 *
 * @param {string} cache
 * @returns {import('stream').Readable}
 */
function lsStream(cache) {
  const indexDir = bucketDir(cache);
  const bucketStream = new BucketStream(cache);

  return glob
    .stream(`${indexDir}/**`, {
      followSymlinkedDirectories: true,
      unique: true,
      onlyFiles: true,
      deep: 2,
      stats: false,
    })
    .pipe(bucketStream);
}
module.exports.lsStream = lsStream;

/**
 *
 * @param {string} cache
 * @returns {Promise<{ [k: string]: FormattedEntry }>}
 */
async function ls(cache) {
  /** @type {{ [k: string]: FormattedEntry }} */
  const res = {};
  for await (const entry of lsStream(cache)) {
    res[entry.key] = entry;
  }
  return res;
}
module.exports.ls = ls;

/**
 *
 * @param {string} cache
 */
function bucketDir(cache) {
  return path.join(cache, `index-v${indexV}`);
}
module.exports._bucketDir = bucketDir;

/**
 *
 * @param {string} cache
 * @param {string} key
 */
function bucketPath(cache, key) {
  const hashed = hashKey(key);
  return path.join(bucketDir(cache), ...hashToSegments(hashed));
}
module.exports._bucketPath = bucketPath;

module.exports._hashKey = hashKey;
function hashKey(key) {
  return hash(key, 'sha256');
}

module.exports._hashEntry = hashEntry;
function hashEntry(str) {
  return hash(str, 'sha1');
}

/**
 *
 * @param {string} str
 * @param {string} digest
 */
function hash(str, digest) {
  return crypto
    .createHash(digest)
    .update(str)
    .digest('hex');
}

/**
 *
 * @param {string} cache
 * @param {Partial<FormattedEntry>} entry
 * @returns {FormattedEntry}
 */
function formatEntry(cache, entry) {
  // Treat null digests as deletions. They'll shadow any previous entries.
  if (!entry || !entry.integrity) {
    return null;
  }
  return {
    key: entry.key,
    integrity: entry.integrity,
    path: contentPath(cache, entry.integrity),
    size: entry.size,
    time: entry.time,
    metadata: entry.metadata,
  };
}
