'use strict';

const path = require('path');
const { promisify } = require('util');

const rimraf = promisify(require('rimraf'));

const index = require('./lib/entry-index');
const memo = require('./lib/memoization');
const rmContent = require('./lib/content/rm');

function entry(cache, key) {
  memo.clearMemoized();
  return index.delete(cache, key);
}
module.exports = entry;
module.exports.entry = entry;

function content(cache, integrity) {
  memo.clearMemoized();
  return rmContent(cache, integrity);
}
module.exports.content = content;

function all(cache) {
  memo.clearMemoized();
  return rimraf(path.join(cache, '*(content-*|index-*)'));
}
module.exports.all = all;
