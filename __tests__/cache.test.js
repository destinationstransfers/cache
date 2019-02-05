'use strict';

const path = require('path');
const rimraf = require('rimraf');
const { randomBytes } = require('crypto');
const { ReadableStreamBuffer } = require('stream-buffers');
const { statSync, existsSync, readFileSync } = require('fs');

const Cache = require('../');

describe('basic cache functions', () => {
  let bigDataBuffer = randomBytes(2048);
  const cachePath = path.join(__dirname, 'test-cache');

  beforeAll(() => {
    rimraf.sync(cachePath);
  });

  test('set -> has -> get -> delete -> has', async () => {
    expect(existsSync(cachePath)).toBeFalsy();

    const cache = new Cache(cachePath);
    // it must create cache directory sync
    expect(statSync(cachePath).isDirectory()).toBeTruthy();
    // but must not throw if another instance want to use the same dir
    expect(() => new Cache(cachePath)).not.toThrow();

    // set a value
    const res = await cache.set('key1', bigDataBuffer, {
      testMeta: 2,
    });
    expect(res.metadata).toHaveProperty('testMeta', 2);
    expect(existsSync(res.path)).toBeTruthy();
    expect(res.size).toBe(bigDataBuffer.byteLength);

    expect(cache.has('key1')).toHaveProperty('integrity', expect.any(String));
    expect(cache.has('key1')).toHaveProperty('time', expect.any(Date));

    expect(bigDataBuffer.compare(await cache.get('key1'))).toBe(0);

    // put the same data as stream
    const rs = new ReadableStreamBuffer();
    rs.put(bigDataBuffer);
    rs.stop();
    await cache.setStream('key2', rs);
    expect(cache.has('key2')).toHaveProperty('path', res.path);

    await cache.delete('key1');
    // should not remove as another key is still pointing to this
    expect(existsSync(res.path)).toBeTruthy();
    expect(cache.has('key1')).toBeFalsy();

    await cache.delete('key2');
    expect(existsSync(res.path)).toBeFalsy();
  });
});
