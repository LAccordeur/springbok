package com.rogerguo.test.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rogerguo.test.store.Chunk;
import org.junit.Test;

import java.util.function.Function;

/**
 * @author yangguo
 * @create 2022-10-08 4:30 PM
 **/
public class CaffeineCacheTest {

    @Test
    public void test() {
        Cache<String, String> cache = Caffeine.newBuilder()
                .maximumSize(200000)
                .build();
        cache.put("k1", "v1");
        cache.get("k1", v -> setValue("k1").apply("k1"));
        cache.invalidate("k1");
        System.out.println(cache.getIfPresent("k2"));

    }

    public Function<String, String> setValue(String key) {
        return t -> key + "value";
    }

}
