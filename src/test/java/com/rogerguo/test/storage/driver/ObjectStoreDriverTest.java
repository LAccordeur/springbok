package com.rogerguo.test.storage.driver;

import com.rogerguo.test.motivation.PutObjects;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;


import static org.junit.Assert.*;

public class ObjectStoreDriverTest {

    @Test
    public void listKeyWithSamePrefix() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);

        System.out.println(driver.listKeysWithSamePrefix("key"));
    }

    @Test
    public void putWithMetadata() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);

        Map<String, String> metadata = new HashMap<>();
        String test = PutObjects.generateRandomString(1024*2-4);
        metadata.put("test", test);

        driver.flush("key-metadata", "testvalue", metadata);
        System.out.println(driver.getMetaDataAsString("key-metadata"));
    }

    @Test
    public void flush() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);

        driver.flush("test-key-1", "test value");

    }

    @Test
    public void testFlush() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);

        driver.flush("test-key-1-byte", "test value".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void getDataAsString() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);
        String result = driver.getDataAsString("test-key-1");
        System.out.println(result);
    }

    @Test
    public void getDataAsByteArray() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);
        byte[] result = driver.getDataAsByteArray("test-key-1-byte");
        System.out.println(new String(result));
    }

    @Test
    public void getDataAsStringPartial() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);
        String result = driver.getDataAsStringPartial("test-key-1", 0, 2);
        System.out.println(result);
    }

    @Test
    public void getDataAsByteArrayPartial() {
        String bucketName = "bucket-api-test-20101010";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);
        byte[] result = driver.getDataAsByteArrayPartial("test-key-1", 0, 3);
        System.out.println(new String(result));
    }

    @Test
    public void getDataAsStringWithRangeWithDuplicatedKeyBatch() {
        List<Triple<String, Integer, Integer>> list = new ArrayList<>();
        Triple<String, Integer, Integer> key = Triple.of("15887.0", 34, 111);
        list.add(key);

        String bucketName = "springbok-store-csv-test";
        Region region = Region.US_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);
        List<String> result = driver.getDataAsStringWithRangeWithDuplicatedKeyBatch(list);
        System.out.println(result.get(0).length());
    }

    @Test
    public void remove() {
    }
}