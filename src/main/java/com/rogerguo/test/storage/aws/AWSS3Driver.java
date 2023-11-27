package com.rogerguo.test.storage.aws;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * from https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/s3/src/main/java/com/example/s3/
 * @author yangguo
 * @create 2021-09-20 9:30 PM
 **/
public class AWSS3Driver {

    private static Logger logger = LoggerFactory.getLogger(AWSS3Driver.class);

    public static void main(String[] args) {
        String bucketName = "flush-test-3333";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        //List<String> keyList = new ArrayList<>();
        //keyList.add("test-key-2");
        //keyList.add("test-key-1");
        //byte[] result = getObjectDataAsByteArrayWithRange(s3Client, bucketName, "test-key", 0, 5);
        deleteBucket(s3Client, bucketName);


        s3Client.close();
    }

    public static boolean doesObjectExist(S3Client s3Client, String bucketName, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try {
            s3Client.headObject(headObjectRequest);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    public static boolean doesBucketExist(S3Client s3Client, String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();

        try {
            s3Client.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    public static void createBucket(S3Client s3Client, String bucketName) {
        if (!doesBucketExist(s3Client, bucketName)) {

            try {
                S3Waiter s3Waiter = s3Client.waiter();
                CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                        .bucket(bucketName)
                        .build();

                s3Client.createBucket(bucketRequest);
                HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                        .bucket(bucketName)
                        .build();

                // Wait until the bucket is created and print out the response.
                WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
                waiterResponse.matched().response().ifPresent(System.out::println);
                logger.info(bucketName + " is ready");

            } catch (S3Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                //System.exit(1);
            }
        } else {
            logger.info("bucket: " + bucketName + " exists");
        }
    }

    /**
     *
     * @param s3Client
     * @param bucketName
     * @param objectKey
     * @param objectPath the path where the file is located
     * @return
     */
    public static String putObjectFromFile(S3Client s3Client, String bucketName, String objectKey, String objectPath) {
        try {

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            PutObjectResponse response = s3Client.putObject(putOb,
                    RequestBody.fromBytes(getObjectFile(objectPath)));

            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            //System.exit(1);
        }
        return "";
    }

    public static void putObjectFromFileAsync(S3AsyncClient s3AsyncClient, String bucketName, String objectKey, String objectPath) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        // Put the object into the bucket
        CompletableFuture<PutObjectResponse> future = s3AsyncClient.putObject(objectRequest,
                AsyncRequestBody.fromFile(Paths.get(objectPath))
        );
        future.whenComplete((resp, err) -> {
            try {
                if (resp != null) {
                    System.out.println("Object uploaded. Details: " + resp);
                } else {
                    // Handle error
                    err.printStackTrace();
                }
            } finally {
                // Only close the client when you are completely done with it
                s3AsyncClient.close();
            }
        });

        future.join();
    }

    public static void checkAndWaitAsyncPutCompletion(List<CompletableFuture<PutObjectResponse>> futureList) {
        for (CompletableFuture<PutObjectResponse> future : futureList) {
            future.join();
        }
    }

    public static CompletableFuture<PutObjectResponse> putObjectFromStringAsync(S3AsyncClient s3AsyncClient, String bucketName, String objectKey, String objectContentString) {

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        //System.out.println("S3 object size: " + objectContentString.length() / 1024.0 + " kb");
        // Put the object into the bucket
        CompletableFuture<PutObjectResponse> future = s3AsyncClient.putObject(objectRequest,
                AsyncRequestBody.fromString(objectContentString)
        );
        future.whenComplete((resp, err) -> {
            try {
                if (resp != null) {
                    //System.out.println("Object uploaded. Details: " + resp);
                } else {
                    // Handle error
                    err.printStackTrace();
                }
            }
            finally {
                // Only close the client when you are completely done with it
                //s3AsyncClient.close();
            }
        });


        return future;
    }

    public static void putObjectFromStringAsyncBatch(S3AsyncClient s3AsyncClient, String bucketName, Map<String, String> objectKVMap) {
        List<CompletableFuture<PutObjectResponse>> futureList = new ArrayList<>();
        for (String key: objectKVMap.keySet()) {
            String value = objectKVMap.get(key);
            CompletableFuture<PutObjectResponse> future =putObjectFromStringAsync(s3AsyncClient, bucketName, key, value);
            futureList.add(future);
        }
        checkAndWaitAsyncPutCompletion(futureList);
    }


    public static String putObjectFromString(S3Client s3Client, String bucketName, String objectKey, String objectContentString) {
        try {

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            PutObjectResponse response = s3Client.putObject(putOb,
                    RequestBody.fromString(objectContentString));
            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            //System.exit(1);
        }
        return "";
    }

    public static String putObjectFromStringWithMetadata(S3Client s3Client, String bucketName, String objectKey, String objectContentString, Map<String, String> metadataMap) {
        try {

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadataMap)
                    .build();

            PutObjectResponse response = s3Client.putObject(putOb,
                    RequestBody.fromString(objectContentString));
            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            //System.exit(1);
        }
        return "";
    }

    public static String putObjectFromByte(S3Client s3Client, String bucketName, String objectKey, byte[] objectContentBytes) {
        try {

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            PutObjectResponse response = s3Client.putObject(putOb,
                    RequestBody.fromBytes(objectContentBytes));

            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            //System.exit(1);
        }
        return "";
    }

    public static Map<String, String> getObjectMetadata(S3Client s3Client, String bucketName, String keyName) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
        return headObjectResponse.metadata();
    }

    public static long getObjectSize(S3Client s3Client, String bucketName, String keyName) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
        return headObjectResponse.contentLength();
    }

    // Return a byte array
    private static byte[] getObjectFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }

    public static byte[] getObjectDataAsByteArray(S3Client s3Client, String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            return objectBytes.asByteArray();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }
        return new byte[]{};
    }

    /**
     * https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
     * @param s3Client
     * @param bucketName
     * @param keyName
     * @param start
     * @param end
     * @return
     */
    public static byte[] getObjectDataAsByteArrayWithRange(S3Client s3Client, String bucketName, String keyName, long start, long end) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .range("bytes=" + start + "-" + end)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            return objectBytes.asByteArray();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }
        return new byte[]{};
    }

    public static String getObjectDataAsString(S3Client s3Client, String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            return new String(data);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }
        return "";
    }





    public static Map<String, String> checkAndWaitAsyncGetCompletion(Map<String, CompletableFuture<ResponseBytes<GetObjectResponse>>> futureMap) {
        Map<String, String> dataStringMap = new HashMap<>();
        for (String key : futureMap.keySet()) {
            CompletableFuture<ResponseBytes<GetObjectResponse>> future = futureMap.get(key);
            future.join();
            try {
                String dataString = new String(future.get().asByteArray());
                dataStringMap.put(key, dataString);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        return dataStringMap;
    }

    public static CompletableFuture<ResponseBytes<GetObjectResponse>> getObjectDataAsStringAsync(S3AsyncClient s3AsyncClient, String bucketName, String keyName) {
        CompletableFuture<ResponseBytes<GetObjectResponse>> futureGet = null;

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            futureGet = s3AsyncClient.getObject(objectRequest,
                    AsyncResponseTransformer.toBytes());

            futureGet.whenComplete((resp, err) -> {
                try {
                    if (resp != null) {
                        //System.out.println("Object downloaded. Details: " + resp);
                    } else {
                        err.printStackTrace();
                    }
                } finally {
                    // Only close the client when you are completely done with it.
                    //s3AsyncClient.close();
                }
            });
            //futureGet.join();
            //futureGet.get().toString();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }

        if (futureGet == null) {
            System.out.println("[getObjectDataAsStringAsync] null future for async get");
        }

        return futureGet;
    }

    public static CompletableFuture<ResponseBytes<GetObjectResponse>> getObjectDataAsStringWithRangeAsync(S3AsyncClient s3AsyncClient, String bucketName, String keyName, long start, long end) {
        CompletableFuture<ResponseBytes<GetObjectResponse>> futureGet = null;

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .range("bytes=" + start + "-" + end)
                    .build();

            futureGet = s3AsyncClient.getObject(objectRequest,
                    AsyncResponseTransformer.toBytes());

            futureGet.whenComplete((resp, err) -> {
                try {
                    if (resp != null) {
                        //System.out.println("Object downloaded. Details: " + resp);
                    } else {
                        err.printStackTrace();
                    }
                } finally {
                    // Only close the client when you are completely done with it.
                    //s3AsyncClient.close();
                }
            });
            //futureGet.join();
            //futureGet.get().toString();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }

        if (futureGet == null) {
            System.out.println("[getObjectDataAsStringAsync] null future for async get");
        }

        return futureGet;
    }

    public static Map<String, String> getObjectDataAsStringAsyncBatch(S3AsyncClient s3AsyncClient, String bucketName, List<String> keyList) {
        Map<String, CompletableFuture<ResponseBytes<GetObjectResponse>>> futureMap = new HashMap<>();
        for (String key: keyList) {
            CompletableFuture<ResponseBytes<GetObjectResponse>> future = getObjectDataAsStringAsync(s3AsyncClient, bucketName, key);
            futureMap.put(key, future);
        }
        return checkAndWaitAsyncGetCompletion(futureMap);
    }

    public static Map<String, String> checkAndWaitAsyncGetCompletionHybridFullAndRange(Map<String, CompletableFuture<ResponseBytes<GetObjectResponse>>> futureMap) {
        Map<String, String> dataStringMap = new HashMap<>();
        for (String key : futureMap.keySet()) {
            CompletableFuture<ResponseBytes<GetObjectResponse>> future = futureMap.get(key);
            future.join();

            try {
                String dataString = new String(future.get().asByteArray());
                dataStringMap.put(key, dataString);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        return dataStringMap;
    }

    /**
     * if we want to get mutiple blocks in the same data object, we cannot use this method, since only the last blocks are remained in this method
     * @param s3AsyncClient
     * @param bucketName
     * @param keyList  The first parameter in the triple is the object key, the second and third ones are the range offset and length (if offset == -1, it means we fetch the whole object)
     * @return
     */
    public static Map<String, String> getObjectDataAsStringAsyncHybridFullAndRangeBatch(S3AsyncClient s3AsyncClient, String bucketName, List<Triple<String, Integer, Integer>> keyList) {
        Map<String, CompletableFuture<ResponseBytes<GetObjectResponse>>> futureMap = new HashMap<>();
        for (Triple<String, Integer, Integer> key: keyList) {
            if (key.getMiddle() != -1) {
                CompletableFuture<ResponseBytes<GetObjectResponse>> future = getObjectDataAsStringWithRangeAsync(s3AsyncClient, bucketName, key.getLeft(), key.getMiddle(), key.getRight());
                futureMap.put(key.getLeft(), future);
            } else {
                CompletableFuture<ResponseBytes<GetObjectResponse>> future = getObjectDataAsStringAsync(s3AsyncClient, bucketName, key.getLeft());
                futureMap.put(key.getLeft(), future);
            }

        }
        return checkAndWaitAsyncGetCompletionHybridFullAndRange(futureMap);
    }

    public static List<String> checkAndWaitAsyncGetCompletionRange(List<CompletableFuture<ResponseBytes<GetObjectResponse>>> futureList) {
        List<String> dataStringList = new ArrayList<>();
        for (CompletableFuture<ResponseBytes<GetObjectResponse>> future : futureList) {

            future.join();

            try {
                String dataString = new String(future.get().asByteArray());
                dataStringList.add(dataString);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        return dataStringList;
    }

    public static List<String> getObjectDataAsStringAsyncWithRangeBatch(S3AsyncClient s3AsyncClient, String bucketName, List<Triple<String, Integer, Integer>> keyList) {
        List<CompletableFuture<ResponseBytes<GetObjectResponse>>> futureList = new ArrayList<>();
        for (Triple<String, Integer, Integer> key : keyList) {
            CompletableFuture<ResponseBytes<GetObjectResponse>> future = getObjectDataAsStringWithRangeAsync(s3AsyncClient, bucketName, key.getLeft(), key.getMiddle(), key.getRight());
            futureList.add(future);
        }
        return checkAndWaitAsyncGetCompletionRange(futureList);
    }


    /**
     *
     * @param s3Client
     * @param bucketName
     * @param keyName
     * @param start
     * @param end inclusive
     * @return
     */
    public static String getObjectDataAsStringWithRange(S3Client s3Client, String bucketName, String keyName, long start, long end) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .range("bytes=" + start + "-" + end)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            return new String(data);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public static String getObjectContentType(S3Client s3Client, String bucketName, String keyName) {
        try {
            HeadObjectRequest objectRequest = HeadObjectRequest.builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            HeadObjectResponse objectHead = s3Client.headObject(objectRequest);
            String type = objectHead.contentType();
            logger.info("The object content type is "+type);
            return type;
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public static void getObjectTags(S3Client s3Client, String bucketName, String keyName) {
        try {
            GetObjectTaggingRequest getTaggingRequest = GetObjectTaggingRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            GetObjectTaggingResponse tags = s3Client.getObjectTagging(getTaggingRequest);
            List<Tag> tagSet= tags.tagSet();

            // Write out the tags
            Iterator<Tag> tagIterator = tagSet.iterator();
            while(tagIterator.hasNext()) {

                Tag tag = (Tag)tagIterator.next();
                System.out.println(tag.key());
                System.out.println(tag.value());
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static String getObjectUrl(S3Client s3Client, String bucketName, String keyName) {
        try {

            GetUrlRequest request = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();

            URL url = s3Client.utilities().getUrl(request);
            logger.info("The URL for  "+keyName +" is "+url.toString());
            return url.toString();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public static List<String> listObjectKeysWithSamePrefix(S3Client s3Client, String bucketName, String prefix) {
        List<String> keyList = new ArrayList<>();

        /*ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        List<S3Object> s3Objects = response.contents();
        for (S3Object s3Object : s3Objects) {
            keyList.add(s3Object.key());
        }*/


        String nextContinuationToken = null;
        try {


            do {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(prefix)
                        .continuationToken(nextContinuationToken);

                ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
                nextContinuationToken = response.nextContinuationToken();

                List<S3Object> objects = response.contents();

                for (S3Object s3Object : objects) {
                    keyList.add(s3Object.key());
                }
            } while (nextContinuationToken != null);

            System.out.println("object num of this prefix in bucket: " + keyList.size() + ", bucket name: " + bucketName);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }
        return keyList;

    }

    public static List<String> listBucketObjects(S3Client s3Client, String bucketName) {
        List<String> keyList = new ArrayList<>();
        String nextContinuationToken = null;
        try {
            /*ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (S3Object s3Object : objects) {
                keyList.add(s3Object.key());
            }*/

            do {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .continuationToken(nextContinuationToken);

                ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
                nextContinuationToken = response.nextContinuationToken();

                List<S3Object> objects = response.contents();

                for (S3Object s3Object : objects) {
                    keyList.add(s3Object.key());
                }
            } while (nextContinuationToken != null);

            System.out.println("object num in bucket: " + keyList.size() + ", bucket name: " + bucketName);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }
        return keyList;
    }

    //convert bytes to kbs
    private static long calKb(Long val) {
        return val/1024;
    }

    public static void deleteObject(S3Client s3Client, String bucketName, String objectKeyName) {
        ArrayList<ObjectIdentifier> toDelete = new ArrayList<ObjectIdentifier>();
        toDelete.add(ObjectIdentifier.builder().key(objectKeyName).build());

        try {
            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(toDelete).build())
                    .build();
            s3Client.deleteObjects(dor);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        logger.info("Deletion " + bucketName + ": " + objectKeyName + " Done!");
    }

    public static void deleteMultipleObjects(S3Client s3Client, String bucketName, List<String> objectKeyNameList) {
        // Upload three sample objects to the specfied Amazon S3 bucket.
        ArrayList<ObjectIdentifier> keys = new ArrayList<>();

        ObjectIdentifier objectId = null;

        for (String keyName : objectKeyNameList) {
            objectId = ObjectIdentifier.builder()
                    .key(keyName)
                    .build();

            keys.add(objectId);
        }

        // Delete multiple objects in one request.
        Delete del = Delete.builder()
                .objects(keys)
                .build();

        try {
            DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(del)
                    .build();

            s3Client.deleteObjects(multiObjectDeleteRequest);
            logger.info("Multiple objects are deleted!");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void deleteBucket(S3Client s3Client, String bucketName) {
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build());
                    System.out.println(s3Object.key() + " is deleted");
                }

                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while(listObjectsV2Response.isTruncated());
            // snippet-end:[s3.java2.s3_bucket_delete.delete_bucket]

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


}
