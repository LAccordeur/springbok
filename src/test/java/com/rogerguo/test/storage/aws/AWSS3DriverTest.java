package com.rogerguo.test.storage.aws;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

import static org.junit.Assert.*;

public class AWSS3DriverTest {

    @Test
    public void getObjectMetadata() {

        S3Client s3Client = S3Client.builder().region(Region.AP_EAST_1).build();
        AWSS3Driver.putObjectFromString(s3Client, "flush-test-1111-from-data", "testmeta", "metadatavalue");
        System.out.println(AWSS3Driver.getObjectSize(s3Client, "flush-test-1111-from-data", "testmeta"));


    }

    @Test
    public void listObjects() {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();
        String bucketName = "flush-test-springbok";
        List<String> result = AWSS3Driver.listBucketObjects(s3, bucketName);
        for (String key : result) {
            System.out.println(key);
        }
    }

    @Test
    public void getEmptyObject() {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();
        String bucketName = "flush-test-springbok";
        String result = AWSS3Driver.getObjectDataAsString(s3, bucketName, "xx");
        System.out.println(result);
    }
}