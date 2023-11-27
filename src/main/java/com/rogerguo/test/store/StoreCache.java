package com.rogerguo.test.store;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * @author yangguo
 * @create 2022-10-08 5:21 PM
 **/
@Deprecated
public class StoreCache {

    private Cache<String, Chunk> chunkCache;

    public StoreCache(int maximumSize) {
        this.chunkCache =  Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .initialCapacity(maximumSize)
                .build();
    }

    public void put(String key, Chunk chunk) {
        chunkCache.put(key, chunk);
    }

    public Chunk get(String key) {
        return chunkCache.getIfPresent(key);
    }

    public void invalidate(String key) {
        chunkCache.invalidate(key);
    }

}
