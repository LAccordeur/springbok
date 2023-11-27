package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.aws.AWSS3Driver;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * @author yangguo
 * @create 2021-10-05 11:07 AM
 **/
public class Init {

    private static String bucketName = "flush-test-1111-from-data";

    private static Region region = Region.AP_EAST_1;

    private static S3Client s3Client = S3Client.builder().region(region).build();

    private static int KB = 1024;

    private static int MB = 1024 * 1024;

    public static void main(String[] args) {

        //createObjects(4096, "T64KB-", 64*KB);
        createBucket();

    }

    public static void createBucket() {
        AWSS3Driver.createBucket(s3Client, bucketName);
    }

    public static void deleteBucket() {
        AWSS3Driver.deleteBucket(s3Client, bucketName);
    }

    public static void createObjects(int num, String keyPrefix, int objectSize) {
        for (int i = 0; i < num; i++) {
            Block block = generateNextObjectData(keyPrefix, objectSize);
            System.out.println(block.getBlockId());
            //System.out.println(block);
            AWSS3Driver.putObjectFromString(s3Client, bucketName, block.getBlockId(), block.getDataString());
        }
    }

    public static int blockCount = 0;
    public static Block generateNextObjectData(String keyPrefix, int objectSize) {
        String dataString = PutObjects.generateRandomString(objectSize);
        Block block = new Block();
        block.setBlockId(keyPrefix + String.format("%02d", blockCount));
        block.setDataString(dataString);
        blockCount++;
        return block;
    }
}

