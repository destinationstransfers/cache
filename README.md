# cache [![codecov](https://codecov.io/gh/destinationstransfers/cache/branch/master/graph/badge.svg)](https://codecov.io/gh/destinationstransfers/cache) ![node](https://img.shields.io/node/v/@destinationstransfers/cache.svg) [![tested with jest](https://img.shields.io/badge/tested_with-jest-99424f.svg)](https://github.com/facebook/jest) [![license](https://img.shields.io/npm/l/@destinationstransfers/cache.svg)](https://npm.im/cacache)
Like [cacache](https://github.com/zkat/cacache), but without localizations and tons of dependencies, while using modern Node features (`async/await`, `fs.promises`, `stream._final` and `stream.pipeline`) and with full JSDoc for VScode IntelliSense.

Uses subresource integrity control. Disk based, no extra memory caching apart of file system cache.

## Usage

```js

const Cache = require('@destinationstransfers/cache');

// to show writable stream example
const assert = require('assert');
const { promisify } = require('util');
const pipeline = promisify(require('stream').pipeline);
const finished = promisify(require('stream').finished);


async fn() {
    const cache = new Cache('someFolderPath');
    const res = await cache.set('key1', buffer);
    /**
     * res => 
     * {
      integrity: string,
      path: string,
      size: number,
      time: Date,
      metadata?: any,
    }
     **/

    const valBuf = await cache.get('key1');
    await cache.delete('key1');

    // The same with Streams
    const res1 = await cache.setStream('key1', someReadable);
    const readable = cache.getStream('key1');
    // or you may get writable stream and push into it
    await pipeline(
        request.get('https:/.....'),
        cache.getWritableStream('cache-url1', { metadata: { url } })
    );
    assert.ok(cache.has('cache-url1').metadata.url, 'Content must be in cache now with associated metadata')
    // or intermittent caching layer
    await finished(
        createReadStream('oldFilename.ext')
            .pipe(cache.createCachingStream('cachingKey'))
            .pipe(createWriteStream('newLocation.ext'))
    )
}
```

Doesn't store the same content twice.
