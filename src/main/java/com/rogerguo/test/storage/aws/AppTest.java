package com.rogerguo.test.storage.aws;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.*;

/**
 * @author yangguo
 * @create 2021-09-21 11:23 AM
 **/
public class AppTest {

    static List<String> keyList = new ArrayList<>();
    public static void main(String[] args) throws IOException {

        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        S3AsyncClient s3AsyncClient =
                S3AsyncClient.crtBuilder()
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .region(region)
                        .build();


        String bucket = "bucket-api-test-" + System.currentTimeMillis();


        tutorialSetup(s3, bucket, region);

        System.out.println("Uploading object...");

        /*s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromString("Testing with the {sdk-java}"));*/

        /*String value = getAlphaNumericString(1024*1024*16);

        CompletableFuture<PutObjectResponse> future = AWSS3Driver.putObjectFromStringAsync(s3AsyncClient, bucket, key, value);
        List<CompletableFuture<PutObjectResponse>> futureList = new ArrayList<>();
        futureList.add(future);
        AWSS3Driver.checkAndWaitAsyncPutCompletion(futureList);*/


        asyncPut(s3AsyncClient, bucket, 50, 1024*1024*8);
        syncPut(s3, bucket, 50, 1024*1024*8);

        asyncGet(s3AsyncClient, bucket, 50);
        //asyncGetHybrid(s3AsyncClient, bucket, 50);
        syncGet(s3, bucket, 50);


        System.out.println("Upload complete");
        System.out.printf("%n");

        cleanUp(s3, bucket, keyList);

        System.out.println("Closing the connection to {S3}");
        s3.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");
    }

    // function to generate a random string of length n
    static String getAlphaNumericString(int n)
    {

        // choose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    public static void syncPut(S3Client s3, String bucketName, int objectNum, int objectSize) {
        String dataString = getAlphaNumericString(objectSize);

        long start = System.currentTimeMillis();
        for (int i = 0; i < objectNum; i++) {
            String keyName = "sync" + i;
            //String dataString = getAlphaNumericString(objectSize);
            AWSS3Driver.putObjectFromString(s3, bucketName, keyName, dataString);
            System.out.println("sync (substring 0-100):" + dataString.substring(0, 100));
            keyList.add(keyName);
        }
        long stop = System.currentTimeMillis();
        System.out.println("sync put takes: " + (stop - start));
    }


    /*public static void asyncPutDirectCall(S3AsyncClient s3, String bucketName, int objectNum, int objectSize) {
        String value = getAlphaNumericString(objectSize);

        long start = System.currentTimeMillis();

        List<CompletableFuture<PutObjectResponse>> futureList = new ArrayList<>();
        for (int i = 0; i < objectNum; i++) {
            String keyName = "async_n" + i;

            CompletableFuture<PutObjectResponse> future = AWSS3Driver.putObjectFromStringAsync(s3, bucketName, keyName, value);
            futureList.add(future);
            System.out.println("async put (substring 0-100): " + value.substring(0, 100));
            keyList.add(keyName);
        }
        //long start = System.currentTimeMillis();
        AWSS3Driver.checkAndWaitAsyncPutCompletion(futureList);
        long stop = System.currentTimeMillis();
        System.out.println("async_native put takes: " + (stop - start));
    }*/


    public static void asyncPut(S3AsyncClient s3, String bucketName, int objectNum, int objectSize) {
        String dataString = getAlphaNumericString(objectSize);
        long start = System.currentTimeMillis();

        Map<String, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < objectNum; i++) {
            String keyName = "async" + i;
            //String dataString = getAlphaNumericString(objectSize);
            keyValueMap.put(keyName, dataString);
            System.out.println("async put (substring 0-100): " + dataString.substring(0, 100));
            keyList.add(keyName);
        }
        //long start = System.currentTimeMillis();
        AWSS3Driver.putObjectFromStringAsyncBatch(s3, bucketName, keyValueMap);
        long stop = System.currentTimeMillis();
        System.out.println("async put takes: " + (stop - start));
    }


    public static void syncGet(S3Client s3, String bucketName, int objectNum) {

        long start = System.currentTimeMillis();
        List<String> valueList = new ArrayList<>();

        for (int i = 0; i < objectNum; i++) {
            String keyName = "sync" + i;
            String value = AWSS3Driver.getObjectDataAsString(s3, bucketName, keyName);
            valueList.add(value);
        }

        long stop = System.currentTimeMillis();

        for (String value : valueList) {
            System.out.println("sync data (substring 0-100): " + value.substring(0, 100));
        }

        System.out.println("sync get takes: " + (stop - start));
    }

    public static void asyncGet(S3AsyncClient s3, String bucketName, int objectNum) {

        long start = System.currentTimeMillis();

        List<String> keyList = new ArrayList<>();
        for (int i = 0; i < objectNum; i++) {
            String keyName = "async" + i;
            keyList.add(keyName);
        }
        Map<String, String> valueList = AWSS3Driver.getObjectDataAsStringAsyncBatch(s3, bucketName, keyList);
        long stop = System.currentTimeMillis();

        for (String value : valueList.keySet()) {
            System.out.println("async data (substring 0-100): " + valueList.get(value).substring(0, 100));
        }

        System.out.println("async get takes: " + (stop - start));
    }

    public static void asyncGetHybrid(S3AsyncClient s3, String bucketName, int objectNum) {

        long start = System.currentTimeMillis();
        Random random = new Random(3);
        List<Triple<String, Integer, Integer>> keyRangeList = new ArrayList<>();
        for (int i = 0; i < objectNum; i++) {
            String keyName = "async" + i;
            if (random.nextInt() % 4 == 0) {
                // full range
                keyRangeList.add(new ImmutableTriple<>(keyName, -1, -1));
            } else {
                keyRangeList.add(new ImmutableTriple<>(keyName, 8, 1024));
            }
        }

        Map<String, String> valueList = AWSS3Driver.getObjectDataAsStringAsyncHybridFullAndRangeBatch(s3, bucketName, keyRangeList);
        long stop = System.currentTimeMillis();

        for (String key : valueList.keySet()) {
            System.out.println("async hybrid - key: " + key + ", value (substring 0-100): " + valueList.get(key).substring(0, 100));
        }
        System.out.println("async hybrid get takes: " + (stop - start));

    }

    public static void tutorialSetup(S3Client s3Client, String bucketName, Region region) {
        try {

            if (region == Region.US_EAST_1) {
                s3Client.createBucket(CreateBucketRequest
                        .builder()
                        .bucket(bucketName)
                        .createBucketConfiguration(
                                CreateBucketConfiguration.builder()
                                        .build())
                        .build());
            } else {
                s3Client.createBucket(CreateBucketRequest
                        .builder()
                        .bucket(bucketName)
                        .createBucketConfiguration(
                                CreateBucketConfiguration.builder()
                                        .locationConstraint(region.id())
                                        .build())
                        .build());
            }
            System.out.println("Creating bucket: " + bucketName);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println(bucketName +" is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void cleanUp(S3Client s3Client, String bucketName, List<String> keyNameList) {
        System.out.println("Cleaning up...");
        try {
            for (String keyName : keyNameList) {
                //System.out.println("Deleting object: " + keyName);
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
                s3Client.deleteObject(deleteObjectRequest);
                //System.out.println(keyName + " has been deleted.");
            }
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName +" has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }
}

