'use strict';

const { promisify } = require('util');

const rimraf = promisify(require('rimraf'));

const contentPath = require('./path');
const { hasContent } = require('./read');

module.exports = rm;
async function rm(cache, integrity) {
  const content = await hasContent(cache, integrity);
  if (content) {
    const sri = content.sri;
    if (sri) {
      await rimraf(contentPath(cache, sri));
      return true;
    }
  } else {
    return false;
  }
}
