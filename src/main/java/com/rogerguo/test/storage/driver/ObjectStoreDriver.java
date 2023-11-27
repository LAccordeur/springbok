package com.rogerguo.test.storage.driver;

import com.rogerguo.test.storage.aws.AWSS3Driver;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.List;
import java.util.Map;

/**
 * store and get data from/to object store like S3
 *
 * Now, each chunk is stored as an individual key-value pair in S3
 *
 * @author yangguo
 * @create 2021-09-20 9:19 PM
 **/
public class ObjectStoreDriver implements PersistenceDriver {

    private String bucketName;  // used to construct uri for specific object

    private Region region;

    private S3Client s3Client;

    private S3AsyncClient s3AsyncClient;

    private String rootDirname = "default";

    public static int getCount = 0;

    public ObjectStoreDriver(String bucketName, Region region) {
        this.bucketName = bucketName;
        this.region = region;
        this.s3Client = S3Client.builder().region(this.region).build();
        this.s3AsyncClient =
                S3AsyncClient.crtBuilder()
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .region(this.region)
                        .build();

        // if the bucket not exists, create it
        AWSS3Driver.createBucket(this.s3Client, this.bucketName);
    }

    public ObjectStoreDriver(String bucketName, Region region, String rootDirname) {
        this.bucketName = bucketName;
        this.region = region;
        this.s3Client = S3Client.builder().region(this.region).build();
        this.rootDirname = rootDirname;
        this.s3AsyncClient =
                S3AsyncClient.crtBuilder()
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .region(this.region)
                        .build();

        // if the bucket not exists, create it
        AWSS3Driver.createBucket(this.s3Client, this.bucketName);
    }

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * S3 does not support append operation, every flush will generate a key-value pair in S3.
     * @param key
     * @param dataString
     */
    @Override
    public void flush(String key, String dataString) {
        AWSS3Driver.putObjectFromString(s3Client, bucketName, key, dataString);
    }

    public void flushBatch(Map<String, String> keyValueMap) {
        AWSS3Driver.putObjectFromStringAsyncBatch(s3AsyncClient, bucketName, keyValueMap);
    }

    @Override
    public void flush(String key, byte[] dataBytes) {
        AWSS3Driver.putObjectFromByte(s3Client, bucketName, key, dataBytes);
    }

    @Override
    public void flush(String key, String dataString, Map<String, String> metadataMap) {
        AWSS3Driver.putObjectFromStringWithMetadata(s3Client, bucketName, key, dataString, metadataMap);
    }

    @Override
    public List<String> listKeysWithSamePrefix(String prefix) {
        getCount++;
        return AWSS3Driver.listObjectKeysWithSamePrefix(s3Client, bucketName, prefix);
    }

    @Override
    public List<String> listKeysInBuckets() {
        getCount++;
        return AWSS3Driver.listBucketObjects(s3Client, bucketName);
    }

    @Override
    public Map<String, String> getMetaDataAsString(String key) {
        getCount++;
        return AWSS3Driver.getObjectMetadata(s3Client, bucketName, key);
    }

    @Override
    public String getDataAsString(String key) {
        getCount++;
        if (!AWSS3Driver.doesObjectExist(s3Client, bucketName, key)) {
            logger.warn("Object [{}] does not exists", key);
            return "";
        }
        return AWSS3Driver.getObjectDataAsString(s3Client, bucketName, key);
    }

    @Override
    public Map<String, String> getDataAsStringBatch(List<String> ketList) {
        return AWSS3Driver.getObjectDataAsStringAsyncBatch(s3AsyncClient, bucketName, ketList);
    }

    @Override
    public Map<String, String> getDataAsStringWithRangeBatch(List<Triple<String, Integer, Integer>> keyList) {
        return AWSS3Driver.getObjectDataAsStringAsyncHybridFullAndRangeBatch(s3AsyncClient, bucketName, keyList);
    }

    @Override
    public List<String> getDataAsStringWithRangeWithDuplicatedKeyBatch(List<Triple<String, Integer, Integer>> keyList) {
        return AWSS3Driver.getObjectDataAsStringAsyncWithRangeBatch(s3AsyncClient, bucketName, keyList);
    }

    @Override
    public byte[] getDataAsByteArray(String key) {
        getCount++;
        if (!AWSS3Driver.doesObjectExist(s3Client, bucketName, key)) {
            logger.warn("Object [{}] does not exists", key);
            return new byte[0];
        }
        return AWSS3Driver.getObjectDataAsByteArray(s3Client, bucketName, key);
    }

    @Override
    public String getDataAsStringPartial(String key, int offset, int length) {
        getCount++;
        if (!AWSS3Driver.doesObjectExist(s3Client, bucketName, key)) {
            logger.warn("Object [{}] does not exists", key);
            return "";
        }
        if (offset < 0 && length < 0) {
            // in this case, we read the whole object
            return AWSS3Driver.getObjectDataAsString(s3Client, bucketName, key);
        }

        return AWSS3Driver.getObjectDataAsStringWithRange(s3Client, bucketName, key, offset, offset+length-1);
    }

    @Override
    public byte[] getDataAsByteArrayPartial(String key, int offset, int length) {
        getCount++;
        if (!AWSS3Driver.doesObjectExist(s3Client, bucketName, key)) {
            logger.warn("Object [{}] does not exists", key);
            return new byte[0];
        }
        if (offset < 0 && length < 0) {
            return AWSS3Driver.getObjectDataAsByteArray(s3Client, bucketName, key);
        }

        return AWSS3Driver.getObjectDataAsByteArrayWithRange(s3Client, bucketName, key, offset, offset+length-1);
    }

    @Override
    public long getObjectSize(String key) {
        getCount++;
        return AWSS3Driver.getObjectSize(s3Client, bucketName, key);
    }

    @Override
    public String getRootUri() {
        return rootDirname;  // the root is bucket for s3
    }

    @Override
    public void remove(String key) {
        AWSS3Driver.deleteObject(s3Client, bucketName, key);
    }

    public void deleteBucket(String bucketName) {
        AWSS3Driver.deleteBucket(s3Client, bucketName);
    }

    @Override
    public void close() {
        s3Client.close();
    }
}
