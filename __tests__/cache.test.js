'use strict';

const path = require('path');
const stream = require('stream');
const { existsSync, writeFileSync, mkdirSync } = require('fs');
const { promisify } = require('util');
const { randomBytes } = require('crypto');
const { execSync } = require('child_process');

const {
  ReadableStreamBuffer,
  WritableStreamBuffer,
} = require('stream-buffers');
const ssri = require('ssri');
const del = require('del');

const pipeline = promisify(stream.pipeline);
const finished = promisify(stream.finished);

// eslint-disable-next-line no-shadow
const Cache = require('../');

describe('basic cache functions', () => {
  const bigDataBuffer = randomBytes(2048);
  const cachePath = path.join(__dirname, 'test-cache');

  beforeAll(() => {
    // for debugging tests in VSCode Jest extension
    if (process.env.CI === 'vscode-jest-tests') jest.setTimeout(10000000);
  });
  beforeEach(() => {
    // https://stackoverflow.com/questions/18052762/remove-directory-which-is-not-empty
    if (existsSync(cachePath)) {
      console.warn('Deleting %s', cachePath);
      const removeDirCmd =
        process.platform === 'win32' ? 'rmdir /s /q ' : 'rm -rf ';
      execSync(
        removeDirCmd + '"' + cachePath + '"',
        console.error.bind(console),
      );
    }
    jest.restoreAllMocks();
  });

  test('set -> has -> get -> delete -> has', async () => {
    expect(existsSync(cachePath)).toBeFalsy();

    const cache = new Cache(cachePath);

    // asks for unknown key
    await expect(cache.get('byaka')).resolves.toBeUndefined();

    // set a value
    const res = await cache.set('key1', bigDataBuffer, {
      testMeta: 2,
    });
    expect(res.metadata).toHaveProperty('testMeta', 2);
    expect(existsSync(res.path)).toBeTruthy();
    expect(res.size).toBe(bigDataBuffer.byteLength);

    expect(cache.has('key1')).toHaveProperty('integrity', expect.any(String));
    expect(cache.has('key1')).toHaveProperty('time', expect.any(Number));

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

  test('write buffer get stream and vice-versa', async () => {
    const cache = new Cache(cachePath);
    await cache.set('key2-1', bigDataBuffer);
    expect(cache.has('key2-1')).toHaveProperty('path');
    const rs = cache.getStream('key2-1');
    expect(rs).toBeInstanceOf(stream.Readable);

    // consume stream
    const buf = [];
    for await (const data of rs) {
      buf.push(data);
    }
    expect(bigDataBuffer.compare(Buffer.concat(buf))).toBe(0);

    await cache.delete('key2-1');
  });

  test('getStream must return stream even for non-existent keys', async () => {
    const cache = new Cache(cachePath);
    const rs = cache.getStream('byaka');
    expect(rs).toBeNull();
  });

  test('data corruption', async () => {
    const cache = new Cache(cachePath);
    const res = await cache.set('testK', bigDataBuffer);
    // corrupting data
    writeFileSync(res.path, 'byaka buke', 'utf8');
    await expect(cache.get('testK')).rejects.toBeDefined();

    // same for stream
    const res2 = await cache.set('test2', bigDataBuffer);
    expect(res.path).toBe(res2.path);

    const rs = cache.getStream('test2');
    expect(rs).toBeInstanceOf(stream.Readable);

    const ws = new WritableStreamBuffer();
    await expect(pipeline(rs, ws)).resolves.toBeUndefined();

    // corrupting data
    writeFileSync(res2.path, randomBytes(2048));
    const rs2 = cache.getStream('test2');
    const ws2 = new WritableStreamBuffer();
    await expect(pipeline(rs2, ws2)).rejects.toHaveProperty(
      'code',
      'EINTEGRITY',
    );
  });

  test('concurency', async () => {
    const cache = new Cache(cachePath);

    const s1 = new ReadableStreamBuffer();
    s1.push(bigDataBuffer);
    s1.stop();

    const s2 = new ReadableStreamBuffer();
    s2.push(bigDataBuffer);
    s2.stop();

    const s3 = new ReadableStreamBuffer();
    s3.push(bigDataBuffer);
    s3.stop();

    const res = await Promise.all([
      cache.set('key1', bigDataBuffer),
      cache.set('key2', bigDataBuffer),
      cache.set('key21', bigDataBuffer),
      cache.set('key22', bigDataBuffer),
      cache.setStream('key3', s1),
      cache.set('key4', bigDataBuffer),
      cache.setStream('key5', s2),
      cache.setStream('key1', s3),
    ]);

    expect(res).toBeInstanceOf(Array);
    expect(res).toHaveLength(8);
    for (const result of res) {
      expect(result.path).toBe(res[0].path);
    }
  });

  test('getWriteStream', async () => {
    const cache = new Cache(cachePath);
    const s1 = new ReadableStreamBuffer();
    s1.push(bigDataBuffer);
    s1.stop();
    const ws = cache.getWriteStream('key2');
    expect(ws).toBeInstanceOf(stream.Writable);
    await pipeline(s1, ws);
    const res = cache.has('key2');
    expect(res).toEqual(
      expect.objectContaining({
        time: expect.any(Number),
        size: bigDataBuffer.byteLength,
        integrity: ssri
          .fromData(bigDataBuffer, { algorithms: ['md5'] })
          .toString(),
      }),
    );
  });

  test('fail to create cache tmp folder', async () => {
    // create cache directory and put `tmp` file in it,
    // so it will be not possible to create tmp dir
    mkdirSync(cachePath, { recursive: true });
    writeFileSync(path.join(cachePath, 'tmp'), 'byaka', 'utf8');
    const cache = new Cache(cachePath);
    expect(() => cache.createCachingStream('key2')).toThrow(
      require('assert').AssertionError,
    );
    // will try to create cache folder where there is a file in the middle
    const cache2 = new Cache(path.join(cachePath, 'tmp', 'here'));
    await expect(cache2.set('key3', bigDataBuffer)).rejects.toHaveProperty(
      'code',
      expect.stringMatching(/ENOTDIR|ENOENT/),
    );
  });

  test('must overwrite existing content with wrong sri', async () => {
    const cache = new Cache(cachePath);
    const sri = ssri.fromData(bigDataBuffer, { algorithms: ['md5'] });
    const filename = path.join(cachePath, sri.hexDigest());
    await cache._ensureCacheDirectory();
    writeFileSync(filename, 'byaka', 'utf8');
    const res = await cache.set('key23', bigDataBuffer);
    // must have the same filename
    expect(res).toHaveProperty('path', filename);
    // but new, correct content
    expect(bigDataBuffer.compare(await cache.get('key23'))).toBe(0);
  });

  test(`can't move file after streaming`, async () => {
    const cache = new Cache(cachePath);

    const s1 = new ReadableStreamBuffer();
    s1.push(bigDataBuffer);
    s1.stop();

    // create folder where we expect to move file
    const sri = ssri.fromData(bigDataBuffer, { algorithms: ['md5'] });
    mkdirSync(path.join(cachePath, sri.hexDigest()), { recursive: true });

    await expect(cache.setStream('key2', s1)).rejects.toHaveProperty(
      'code',
      'EISDIR', // Error: EISDIR: illegal operation on a directory, read
    );
  });

  test('getStream should throw if content failed integrity check', async () => {
    const cache = new Cache(cachePath);
    const sri = ssri.fromData(bigDataBuffer, { algorithms: ['md5'] });
    const filename = path.join(cachePath, sri.hexDigest());
    const res = await cache.set('key23', bigDataBuffer);
    expect(res).toHaveProperty('path', filename);
    // test wrong size first
    writeFileSync(filename, 'byaka', 'utf8');
    // get and consume stream
    /* eslint-disable no-empty */
    await expect(
      (async () => {
        for await (const data of cache.getStream('key23')) {
        }
      })(),
    ).rejects.toHaveProperty('code', 'EBADSIZE');
    // it must revove key after bad content found
    expect(cache.has('key23')).toBeFalsy();

    // same size to avoid size mismatch
    await cache.set('key23', bigDataBuffer);
    writeFileSync(filename, randomBytes(2048));
    // get and consume stream
    await expect(
      (async () => {
        for await (const data of cache.getStream('key23')) {
        }
      })(),
    ).rejects.toHaveProperty('code', 'EINTEGRITY');
    /* eslint-enable no-empty */
    expect(cache.has('key23')).toBeFalsy();
  });

  test('teach ssri stream produce other errors', async () => {
    jest.spyOn(ssri, 'integrityStream').mockImplementation(
      () =>
        new stream.Transform({
          transform() {
            this.emit('error', new Error('Byaka'));
          },
        }),
    );

    const cache = new Cache(cachePath);
    const s1 = new ReadableStreamBuffer();
    s1.push(bigDataBuffer);
    s1.stop();
    await expect(cache.setStream('key', s1)).rejects.toHaveProperty(
      'message',
      'Byaka',
    );
    expect(cache.has('key')).toBeFalsy();

    // try reading
    await cache.set('key2', bigDataBuffer);
    // get and consume stream
    /* eslint-disable no-empty */
    await expect(
      (async () => {
        for await (const data of cache.getStream('key2')) {
        }
      })(),
    ).rejects.toHaveProperty('message', 'Byaka');
    /* eslint-enable no-empty */
    expect(cache.has('key23')).toBeFalsy();

    // write stream
    const ws = cache.getWriteStream('bb');
    expect(() => ws.write('hello', 'utf8')).toThrow('Byaka');
  });

  test('createCachingStream', async () => {
    const cache = new Cache(cachePath);

    const s1 = new ReadableStreamBuffer();
    s1.push(bigDataBuffer);
    s1.stop();

    const ws = new WritableStreamBuffer();

    await finished(s1.pipe(cache.createCachingStream('key3')).pipe(ws));
    expect(cache.has('key3')).toEqual(
      expect.objectContaining({
        time: expect.any(Number),
        size: bigDataBuffer.byteLength,
        integrity: ssri
          .fromData(bigDataBuffer, { algorithms: ['md5'] })
          .toString(),
      }),
    );

    expect(bigDataBuffer.compare(ws.getContents())).toBe(0);
  });

  test('persistence', async () => {
    const cache1 = new Cache(cachePath, true);
    expect(cache1.size).toBe(0);
    await Promise.all([
      cache1.set('key1', bigDataBuffer),
      cache1.set('key2', 'test string'),
      cache1.set('key3', 'string3'),
    ]);

    const cache2 = new Cache(cachePath, true);
    expect(cache2).toHaveProperty('size', 3);
  });
});
