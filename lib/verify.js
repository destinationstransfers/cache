'use strict';

const figgyPudding = require('figgy-pudding');
const glob = require('fast-glob');
const path = require('path');
const PQueue = require('p-queue');
const ssri = require('ssri');
const { createReadStream } = require('fs');
const { promisify } = require('util');
const { stat, unlink, truncate, writeFile, readFile } = require('fs').promises;

const contentPath = require('./content/path');
const fixOwner = require('./util/fix-owner');
const index = require('./entry-index');

const rimraf = promisify(require('rimraf'));

const VerifyOpts = figgyPudding({
  concurrency: {
    default: 20,
  },
  filter: {},
  log: {
    default: { silly() {} },
  },
  uid: {},
  gid: {},
});

module.exports = verify;
async function verify(cache, opts) {
  opts = VerifyOpts(opts);
  opts.log.silly('verify', 'verifying cache at', cache);

  const startTime = process.hrtime.bigint();
  const stats = {};
  let i = 0;
  for (const step of [
    fixPerms,
    garbageCollect,
    rebuildIndex,
    cleanTmp,
    writeVerifile,
  ]) {
    const label = step.name || `step #${i++}`;
    const start = process.hrtime.bigint();
    const s = await step(cache, opts);
    if (s) Object.assign(stats, s);
    const end = process.hrtime.bigint();
    if (!stats.runTime) {
      stats.runTime = {};
    }
    stats.runTime[label] = end - start;
  }

  stats.runTime.total = process.hrtime.bigint() - startTime;
  opts.log.silly(
    'verify',
    'verification finished for',
    cache,
    'in',
    `${stats.runTime.total}ns`,
  );

  return stats;
}

async function fixPerms(cache, opts) {
  opts.log.silly('verify', 'fixing cache permissions');
  await fixOwner.mkdirfix(cache, opts.uid, opts.gid);
  // TODO - fix file permissions too
  await fixOwner.chownr(cache, opts.uid, opts.gid);
  return null;
}

// Implements a naive mark-and-sweep tracing garbage collector.
//
// The algorithm is basically as follows:
// 1. Read (and filter) all index entries ("pointers")
// 2. Mark each integrity value as "live"
// 3. Read entire filesystem tree in `content-vX/` dir
// 4. If content is live, verify its checksum and delete it if it fails
// 5. If content is not marked as live, rimraf it.
//
async function garbageCollect(cache, opts) {
  opts.log.silly('verify', 'garbage collecting content');
  const indexStream = index.lsStream(cache);
  const liveContent = new Set();

  for await (const entry of indexStream) {
    if (opts.filter && !opts.filter(entry)) continue;
    liveContent.add(entry.integrity.toString());
  }

  const contentDir = contentPath._contentDir(cache);
  const stats = {
    verifiedContent: 0,
    reclaimedCount: 0,
    reclaimedSize: 0,
    badContentCount: 0,
    keptSize: 0,
  };

  for await (const f of glob.stream(path.join(contentDir, '**'), {
    followSymlinkedDirectories: false,
    onlyFiles: true,
    deep: true,
  })) {
    const split = f.split(/[/\\]/);
    const digest = split.slice(split.length - 3).join('');
    const algo = split[split.length - 4];
    const integrity = ssri.fromHex(digest, algo);
    if (liveContent.has(integrity.toString())) {
      const info = await verifyContent(f, integrity);
      if (!info.valid) {
        stats.reclaimedCount++;
        stats.badContentCount++;
        stats.reclaimedSize += info.size;
      } else {
        stats.verifiedContent++;
        stats.keptSize += info.size;
      }
    } else {
      // No entries refer to this content. We can delete.
      stats.reclaimedCount++;
      const s = await stat(f);
      await rimraf(f);
      stats.reclaimedSize += s.size;
    }
  }
  return stats;
}

async function verifyContent(filepath, sri) {
  try {
    const st = await stat(filepath);

    const contentInfo = {
      size: st.size,
      valid: true,
    };

    try {
      await ssri.checkStream(createReadStream(filepath), sri);
    } catch (err) {
      if (err.code !== 'EINTEGRITY') {
        throw err;
      }
      await unlink(filepath);
      contentInfo.valid = false;
    }

    return contentInfo;
  } catch (err) {
    if (err.code === 'ENOENT') return { size: 0, valid: false };
    throw err;
  }
}

async function rebuildIndex(cache, opts) {
  opts.log.silly('verify', 'rebuilding index');
  const entries = await index.ls(cache);

  const stats = {
    missingContent: 0,
    rejectedEntries: 0,
    totalEntries: 0,
  };
  const buckets = {};
  for (const k in entries) {
    if (entries.hasOwnProperty(k)) {
      const hashed = index._hashKey(k);
      const entry = entries[k];
      const excluded = opts.filter && !opts.filter(entry);
      if (excluded) stats.rejectedEntries++;
      if (buckets[hashed] && !excluded) {
        buckets[hashed].push(entry);
      } else if (buckets[hashed] && excluded) {
        // skip
      } else if (excluded) {
        buckets[hashed] = [];
        buckets[hashed]._path = index._bucketPath(cache, k);
      } else {
        buckets[hashed] = [entry];
        buckets[hashed]._path = index._bucketPath(cache, k);
      }
    }
  }

  const queue = new PQueue({ concurrency: opts.concurrency });

  for (const key in buckets) {
    queue.add(() => rebuildBucket(cache, buckets[key], stats, opts));
  }

  await queue.onEmpty();

  return stats;
}

async function rebuildBucket(cache, bucket, stats, opts) {
  await truncate(bucket._path);
  // This needs to be serialized because cache explicitly
  // lets very racy bucket conflicts clobber each other.
  for (const entry of bucket) {
    const content = contentPath(cache, entry.integrity);
    try {
      await stat(content);
      await index.insert(cache, entry.key, entry.integrity, {
        uid: opts.uid,
        gid: opts.gid,
        metadata: entry.metadata,
        size: entry.size,
      });
      stats.totalEntries++;
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      stats.rejectedEntries++;
      stats.missingContent++;
    }
  }
}

async function cleanTmp(cache, opts) {
  opts.log.silly('verify', 'cleaning tmp directory');
  await rimraf(path.join(cache, 'tmp'));
}

function writeVerifile(cache, opts) {
  const verifile = path.join(cache, '_lastverified');
  opts.log.silly('verify', 'writing verifile to ' + verifile);
  return writeFile(verifile, '' + +new Date());
}

module.exports.lastRun = lastRun;
async function lastRun(cache) {
  const data = await readFile(path.join(cache, '_lastverified'), 'utf8');
  return new Date(+data);
}
