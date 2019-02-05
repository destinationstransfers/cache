'use strict';

const get = require('./get.js');
const ls = require('./ls.js');
const put = require('./put.js');
const rm = require('./rm.js');
const tmp = require('./lib/util/tmp');
const verify = require('./lib/verify');
const { clearMemoized } = require('./lib/memoization');

const x = module.exports;

x.ls = cache => ls(cache);
x.ls.stream = cache => ls.stream(cache);

x.get = (cache, key, opts) => get(cache, key, opts);
x.get.byDigest = (cache, hash, opts) => get.byDigest(cache, hash, opts);
x.get.stream = (cache, key, opts) => get.stream(cache, key, opts);
x.get.stream.byDigest = (cache, hash, opts) =>
  get.stream.byDigest(cache, hash, opts);
x.get.copy = (cache, key, dest, opts) => get.copy(cache, key, dest, opts);
x.get.copy.byDigest = (cache, hash, dest, opts) =>
  get.copy.byDigest(cache, hash, dest, opts);
x.get.info = (cache, key) => get.info(cache, key);
x.get.hasContent = (cache, hash) => get.hasContent(cache, hash);

x.put = (cache, key, data, opts) => put(cache, key, data, opts);
x.put.stream = (cache, key, opts) => put.stream(cache, key, opts);

x.rm = (cache, key) => rm.entry(cache, key);
x.rm.all = cache => rm.all(cache);
x.rm.entry = x.rm;
x.rm.content = (cache, hash) => rm.content(cache, hash);

x.clearMemoized = () => clearMemoized();

x.tmp = {};
x.tmp.mkdir = (cache, opts) => tmp.mkdir(cache, opts);
x.tmp.withTmp = (cache, opts, cb) => tmp.withTmp(cache, opts, cb);

x.verify = (cache, opts) => verify(cache, opts);
x.verify.lastRun = cache => verify.lastRun(cache);
