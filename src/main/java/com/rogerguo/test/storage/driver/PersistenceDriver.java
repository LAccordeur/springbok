package com.rogerguo.test.storage.driver;

import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Date 2021/4/25 11:14
 * @Created by GUO Yang
 */
public interface PersistenceDriver {

    // only for test; key is file path for disk file and object key name for S3
    void flush(String key, String dataString);

    void flushBatch(Map<String, String> keyValueMap);

    void flush(String key, String dataString, Map<String, String> metadataMap);

    void flush(String key, byte[] dataBytes);

    String getDataAsString(String key);

    Map<String, String> getDataAsStringBatch(List<String> ketList);

    Map<String, String> getDataAsStringWithRangeBatch(List<Triple<String, Integer, Integer>> keyList);

    List<String> getDataAsStringWithRangeWithDuplicatedKeyBatch(List<Triple<String, Integer, Integer>> keyList);

    Map<String, String> getMetaDataAsString(String key);

    List<String> listKeysWithSamePrefix(String prefix);

    List<String> listKeysInBuckets();

    byte[] getDataAsByteArray(String key);

    String getDataAsStringPartial(String key, int offset, int length);

    byte[] getDataAsByteArrayPartial(String key, int offset, int length);

    long getObjectSize(String key);

    void remove(String key);

    String getRootUri();

    void close();
}
