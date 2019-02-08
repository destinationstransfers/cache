

interface CacheEntity {
    integrity: string,
    path: string,
    size: number,
    time: number,
    metadata?: any,
}

declare class DestCache<string, boolean> extends Map<string, CacheEntity> {
    constructor(cachePath: string, persistent?: boolean);
    set: (key: string, data: string | Buffer, metadata?: Object) => Promise.<CacheEntity>;
    delete: (key: string) => Promise.<boolean>;
    get: (key: string) => Promise.<buffer>;
    has: (key: string) => false | CacheEntity;
    getWriteStream: (key: string, metadata?: Object) => import('stream').Writable;
    createCachingStream: (key: string, metadata?: Object) => import('stream').Transform;
    setStream: (key: string, stream: import('stream').Readable, metadata?: Object) => Promise.<CacheEntity>;
    getStream: (key: string) => import('stream').Readable;
}

export = DestCache;

