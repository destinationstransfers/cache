'use strict';

const figgyPudding = require('figgy-pudding');
const ssri = require('ssri');
const { createReadStream } = require('fs');
const { lstat, readFile, copyFile } = require('fs').promises;
const { PassThrough, pipeline } = require('stream');

const contentPath = require('./path');

const ReadOpts = figgyPudding({
  size: {},
});

module.exports = read;
function read(cache, integrity, opts) {
  opts = ReadOpts(opts);
  return withContentSri(cache, integrity, async (cpath, sri) => {
    const data = await readFile(cpath);

    if (typeof opts.size === 'number' && opts.size !== data.length) {
      throw sizeError(opts.size, data.length);
    } else if (ssri.checkData(data, sri)) {
      return data;
    } else {
      throw integrityError(sri, cpath);
    }
  });
}

function readStream(cache, integrity, opts) {
  opts = ReadOpts(opts);
  const stream = new PassThrough();
  withContentSri(cache, integrity, (cpath, sri) => {
    return lstat(cpath).then(stat => ({ cpath, sri, stat }));
  })
    .then(({ cpath, sri, stat }) =>
      pipeline(
        createReadStream(cpath),
        ssri.integrityStream({
          integrity: sri,
          size: opts.size,
        }),
        stream,
        err => {
          if (err) stream.emit('error', err);
        },
      ),
    )
    .catch(err => {
      stream.emit('error', err);
    });
  return stream;
}
module.exports.stream = readStream;
module.exports.readStream = readStream;

function copy(cache, integrity, dest, opts) {
  opts = ReadOpts(opts);
  return withContentSri(cache, integrity, (cpath, sri) => {
    return copyFile(cpath, dest);
  });
}
module.exports.copy = copy;

/**
 *
 * @param {string} cache
 * @param {string} integrity
 * @returns {Promise.<{ size: number, sri: string, stat: import('fs').Stats } | boolean>}
 */
async function hasContent(cache, integrity) {
  if (!integrity) return false;

  return withContentSri(cache, integrity, async (cpath, sri) => {
    try {
      const stat = await lstat(cpath);
      return { size: stat.size, sri, stat };
    } catch (err) {
      if (err.code === 'ENOENT') {
        return false;
      }
      if (err.code === 'EPERM') {
        if (process.platform !== 'win32') {
          throw err;
        } else {
          return false;
        }
      }
    }
  });
}
module.exports.hasContent = hasContent;

/**
 *
 * @param {string} cache
 * @param {string} integrity
 * @param {(cpath: string, digest: string) => any} fn
 */
async function withContentSri(cache, integrity, fn) {
  const sri = ssri.parse(integrity);
  // If `integrity` has multiple entries, pick the first digest
  // with available local data.
  const algo = sri.pickAlgorithm();
  const digests = sri[algo];
  if (digests.length <= 1) {
    const cpath = contentPath(cache, digests[0]);
    return fn(cpath, digests[0]);
  } else {
    let lastErr = null;
    for (const meta of sri[sri.pickAlgorithm()]) {
      try {
        return await withContentSri(cache, meta, fn);
      } catch (err) {
        lastErr = err;
      }
    }
    if (lastErr) {
      throw lastErr;
    }
  }
}

function sizeError(expected, found) {
  const err = new Error(
    `Bad data size: expected inserted data to be ${expected} bytes, but got ${found} instead`,
  );
  err.expected = expected;
  err.found = found;
  err.code = 'EBADSIZE';
  return err;
}

function integrityError(sri, path) {
  const err = new Error(`Integrity verification failed for ${sri} (${path})`);
  err.code = 'EINTEGRITY';
  err.sri = sri;
  err.path = path;
  return err;
}
