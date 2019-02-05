'use strict';

const { pipe, through } = require('mississippi');
const { pipeline, PassThrough, finished } = require('stream');
const { writeFile } = require('fs').promises;

const figgyPudding = require('figgy-pudding');
const index = require('./lib/entry-index');
const memo = require('./lib/memoization');
const read = require('./lib/content/read');

const GetOpts = figgyPudding({
  integrity: {},
  memoize: {},
  size: {},
});

module.exports = function get(cache, key, opts) {
  return getData(false, cache, key, opts);
};
module.exports.byDigest = function getByDigest(cache, digest, opts) {
  return getData(true, cache, digest, opts);
};
async function getData(byDigest, cache, key, opts) {
  opts = GetOpts(opts);
  const memoized = byDigest
    ? memo.get.byDigest(cache, key, opts)
    : memo.get(cache, key, opts);
  if (memoized && opts.memoize !== false) {
    return byDigest
      ? memoized
      : {
          metadata: memoized.entry.metadata,
          data: memoized.data,
          integrity: memoized.entry.integrity,
          size: memoized.entry.size,
        };
  }
  const entry = !byDigest && (await index.find(cache, key, opts));
  if (!entry && !byDigest) {
    throw new index.NotFoundError(cache, key);
  }
  const data = await read(cache, byDigest ? key : entry.integrity, {
    integrity: opts.integrity,
    size: opts.size,
  });
  const res = byDigest
    ? data
    : {
        metadata: entry.metadata,
        data: data,
        size: entry.size,
        integrity: entry.integrity,
      };
  if (opts.memoize && byDigest) {
    memo.put.byDigest(cache, key, res, opts);
  } else if (opts.memoize) {
    memo.put(cache, entry, res.data, opts);
  }
  return res;
}

module.exports.stream = getStream;
function getStream(cache, key, opts) {
  opts = GetOpts(opts);
  let stream = new PassThrough({ objectMode: true });
  const memoized = memo.get(cache, key, opts);
  if (memoized && opts.memoize !== false) {
    stream.on('newListener', (ev, cb) => {
      switch (ev) {
        case 'metadata':
          return cb(memoized.entry.metadata);
        case 'integrity':
          return cb(memoized.entry.integrity);
        case 'size':
          return cb(memoized.entry.size);
      }
    });
    stream.write(memoized.data, () => stream.end());
    return stream;
  }
  index
    .find(cache, key)
    .then(entry => {
      if (!entry) {
        return stream.emit('error', new index.NotFoundError(cache, key));
      }
      let memoStream;
      if (opts.memoize) {
        let memoData = [];
        let memoLength = 0;
        memoStream = new PassThrough({
          transform(chunk, encoding, callback) {
            memoData.push(chunk);
            memoLength += chunk.length;
            callback(null, chunk);
          },
        });
        finished(memoStream, err => {
          if (!err)
            memo.put(cache, entry, Buffer.concat(memoData, memoLength), opts);
        });
      } else {
        memoStream = new PassThrough();
      }
      stream.emit('metadata', entry.metadata);
      stream.emit('integrity', entry.integrity);
      stream.emit('size', entry.size);
      stream.on('newListener', (ev, cb) => {
        switch (ev) {
          case 'metadata':
            return cb(entry.metadata);
          case 'integrity':
            return cb(entry.integrity);
          case 'size':
            return cb(entry.size);
        }
      });
      pipeline(
        read.readStream(
          cache,
          entry.integrity,
          opts.concat({
            size: opts.size == null ? entry.size : opts.size,
          }),
        ),
        memoStream,
        stream,
        err => {
          if (err) stream.emit('error', err);
        },
      );
    })
    .catch(err => stream.emit('error', err));
  return stream;
}

module.exports.stream.byDigest = getStreamDigest;
function getStreamDigest(cache, integrity, opts) {
  opts = GetOpts(opts);
  const memoized = memo.get.byDigest(cache, integrity, opts);
  if (memoized && opts.memoize !== false) {
    const stream = through();
    stream.write(memoized, () => stream.end());
    return stream;
  } else {
    let stream = read.readStream(cache, integrity, opts);
    if (opts.memoize) {
      let memoData = [];
      let memoLength = 0;
      const memoStream = through(
        (c, en, cb) => {
          memoData && memoData.push(c);
          memoLength += c.length;
          cb(null, c, en);
        },
        cb => {
          memoData &&
            memo.put.byDigest(
              cache,
              integrity,
              Buffer.concat(memoData, memoLength),
              opts,
            );
          cb();
        },
      );
      pipeline(stream, memoStream, err => {
        if (err) stream.emit('error', err);
      });
    }
    return stream;
  }
}

module.exports.info = info;
function info(cache, key, opts) {
  opts = GetOpts(opts);
  const memoized = memo.get(cache, key, opts);
  if (memoized && opts.memoize !== false) {
    return Promise.resolve(memoized.entry);
  } else {
    return index.find(cache, key);
  }
}

module.exports.hasContent = read.hasContent;

module.exports.copy = function cp(cache, key, dest, opts) {
  return copy(false, cache, key, dest, opts);
};
module.exports.copy.byDigest = function cpDigest(cache, digest, dest, opts) {
  return copy(true, cache, digest, dest, opts);
};
async function copy(byDigest, cache, key, dest, opts) {
  opts = GetOpts(opts);
  if (read.copy) {
    const entry = await (byDigest
      ? Promise.resolve(null)
      : index.find(cache, key, opts));

    if (!entry && !byDigest) {
      throw new index.NotFoundError(cache, key);
    }

    await read.copy(cache, byDigest ? key : entry.integrity, dest, opts);

    return byDigest
      ? key
      : {
          metadata: entry.metadata,
          size: entry.size,
          integrity: entry.integrity,
        };
  } else {
    const res = await getData(byDigest, cache, key, opts);
    await writeFile(dest, byDigest ? res : res.data);
    return byDigest
      ? key
      : {
          metadata: res.metadata,
          size: res.size,
          integrity: res.integrity,
        };
  }
}
