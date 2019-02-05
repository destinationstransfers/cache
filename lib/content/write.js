'use strict';

const path = require('path');
const ssri = require('ssri');
const uniqueFilename = require('unique-filename');
const { createWriteStream } = require('fs');
const { PassThrough, pipeline } = require('stream');
const { promisify } = require('util');
const { to } = require('mississippi');
const { writeFile, mkdir } = require('fs').promises;

const contentPath = require('./path');
const moveFile = require('../util/move-file');

const rimraf = promisify(require('rimraf'));
const pipe = promisify(pipeline);

module.exports = write;
/**
 *
 * @param {string} cache
 * @param {*} data
 * @param {{ algorithms?: string[], size?: number, integrity?: string }} [opts]
 * @returns {Promise.<{ integrity: string, size: number }>}
 */
async function write(cache, data, opts = {}) {
  if (Array.isArray(opts.algorithms) && opts.algorithms.length > 1) {
    throw new Error(`opts.algorithms only supports a single algorithm for now`);
  }
  if (typeof opts.size === 'number' && data.length !== opts.size) {
    throw sizeError(opts.size, data.length);
  }
  const sri = ssri.fromData(data, {
    algorithms: opts.algorithms,
  });
  if (opts.integrity && !ssri.checkData(data, opts.integrity, opts)) {
    throw checksumError(opts.integrity, sri);
  }
  const tmp = await makeTmp(cache, opts);
  await writeFile(tmp, data, { flag: 'wx' });
  await moveToDestination(tmp, cache, sri, opts);
  return { integrity: sri, size: data.length };
}

function writeStream(cache, opts) {
  opts = opts || {};
  const inputStream = new PassThrough();
  let inputErr = false;
  function errCheck() {
    if (inputErr) {
      throw inputErr;
    }
  }

  let allDone;
  const ret = to(
    (c, n, cb) => {
      if (!allDone) {
        allDone = handleContent(inputStream, cache, opts, errCheck);
      }
      inputStream.write(c, n, cb);
    },
    cb => {
      inputStream.end(() => {
        if (!allDone) {
          const e = new Error(`Cache input stream was empty`);
          e.code = 'ENODATA';
          return ret.emit('error', e);
        }
        allDone.then(
          res => {
            res.integrity && ret.emit('integrity', res.integrity);
            res.size !== null && ret.emit('size', res.size);
            cb();
          },
          e => {
            ret.emit('error', e);
          },
        );
      });
    },
  );
  ret.once('error', e => {
    inputErr = e;
  });
  return ret;
}
module.exports.stream = writeStream;

/**
 *
 * @param {import('stream').Readable} inputStream
 * @param {string} cache
 * @param {*} opts
 */
async function handleContent(inputStream, cache, opts) {
  let tmp;
  let res;
  tmp = await makeTmp(cache, opts);
  res = await pipeToTmp(inputStream, cache, tmp, opts);
  await moveToDestination(tmp, cache, res.integrity, opts);
  return res;
}

/**
 *
 * @param {import('stream').Readable} inputStream
 * @param {string} cache
 * @param {string} tmpTarget
 * @param {*} opts
 * @returns {Promise.<{ integrity: string, size: number }>}
 */
async function pipeToTmp(inputStream, cache, tmpTarget, opts) {
  let integrity;
  let size;

  try {
    await pipe(
      inputStream,
      ssri
        .integrityStream({
          integrity: opts.integrity,
          algorithms: opts.algorithms,
          size: opts.size,
        })
        .on('integrity', s => {
          integrity = s;
        })
        .on('size', s => {
          size = s;
        }),
      createWriteStream(tmpTarget, {
        flags: 'wx',
      }),
    );
    return { integrity, size };
  } catch (err) {
    await rimraf(tmpTarget);
    throw err;
  }
}

/**
 *
 * @param {string} cache
 * @param {*} opts
 * @returns {Promise.<string>} - temporary file name
 */
async function makeTmp(cache, opts = {}) {
  const tempDirectory = path.join(cache, 'tmp');
  // ensure directory exists
  try {
    await mkdir(tempDirectory, { recursive: true });
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }
  return uniqueFilename(tempDirectory, opts.tmpPrefix);
}

async function moveToDestination(tmp, cache, sri) {
  const destination = contentPath(cache, sri);
  const destDir = path.dirname(destination);

  try {
    await mkdir(destDir, { recursive: true });
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }
  await moveFile(tmp.target, destination);
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

function checksumError(expected, found) {
  const err = new Error(`Integrity check failed:
  Wanted: ${expected}
   Found: ${found}`);
  err.code = 'EINTEGRITY';
  err.expected = expected;
  err.found = found;
  return err;
}
