package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.aws.AWSS3Driver;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Description
 * @Date 2021/10/9 17:28
 * @Created by rogerguo
 */
public class PutObjectsInParallel {

    private static int MAX_THRESHOLDS_FOR_UPLOADS = 4;

    static ExecutorService executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_UPLOADS);

    private static int KB = 1024;

    private static int MB = 1024 * 1024;

    public static void main(String[] args) {
        testParallelReadBatchForSmallObject();
    }

    public static void testParallelReadBatchForSmallObject() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512);
        List<String> keyPrefixList = Arrays.asList("UP1KB-", "UP8KB-", "UP64KB-", "UP512KB-");
        Map<String, Integer> objectSizeMap = new HashMap<>();
        objectSizeMap.put("UP1KB-", 1*KB);
        objectSizeMap.put("UP8KB-", 8*KB);
        objectSizeMap.put("UP64KB-", 64*KB);
        objectSizeMap.put("UP512KB-", 512*KB);

        for (String keyPrefix : keyPrefixList) {
            for (int threadNum : threadNumList) {
                MAX_THRESHOLDS_FOR_UPLOADS = threadNum;
                executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_UPLOADS);

                if (keyPrefix.equals("UP512KB-") && threadNum == 512) {
                    continue;
                }

                List<String> keyList = new ArrayList<>();
                for (int i = 0; i < threadNum; i++) {
                    String key = keyPrefix + String.format("%02d",i);
                    keyList.add(key);
                }
                String logName = String.format("parallel-upload-%s-%d-thread.log", keyPrefix, threadNum);
                double result = testUploadObjectsInParallel(s3Client, bucketName, keyList, objectSizeMap.get(keyPrefix), 2, logName);
                System.out.println("average latency: " + result);
                executorService.shutdown();
            }
        }

    }


    public static void testParallelUploadBatchForNotSmallObject() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64);
        List<String> keyPrefixList = Arrays.asList("UP4MB-", "UP8MB-", "UP16MB-", "UP32MB-");
        Map<String, Integer> objectSizeMap = new HashMap<>();
        objectSizeMap.put("UP4MB-", 4*MB);
        objectSizeMap.put("UP8MB-", 8*MB);
        objectSizeMap.put("UP16MB-", 16*MB);
        objectSizeMap.put("UP32MB-", 32*MB);

        for (String keyPrefix : keyPrefixList) {
            for (int threadNum : threadNumList) {
                MAX_THRESHOLDS_FOR_UPLOADS = threadNum;
                executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_UPLOADS);

                List<String> keyList = new ArrayList<>();
                for (int i = 0; i < threadNum; i++) {
                    String key = keyPrefix + String.format("%02d",i);
                    keyList.add(key);
                }
                String logName = String.format("parallel-upload-%s-%d-thread.log", keyPrefix, threadNum);
                double result = testUploadObjectsInParallel(s3Client, bucketName, keyList, objectSizeMap.get(keyPrefix) , 1, logName);
                System.out.println("average latency: " + result);
                executorService.shutdown();
            }
        }

    }

    public static double testUploadObjectsInParallel(S3Client s3Client, String bucketName, List<String> keyNameList, int objectSize, int times, String logFilename) {
        List<Long> latencyList = new ArrayList<>();
        SaveStatisticUtils.saveResultToFile(keyNameList + "\n", logFilename);
        String content = PutObjects.generateRandomString(objectSize);
        for (int i = 0; i < times; i++) {
            System.out.println("-----------------------------------" + i);
            long latency = uploadObjectsInParallel(s3Client, bucketName, keyNameList, content);
            latencyList.add(latency);
        }

        System.out.println(latencyList);
        int length = latencyList.size();
        double averageLatency = GetObjects.calculateAverage(latencyList.subList(1, length));
        double standardDiviation = GetObjects.calculateStandardDiviation(latencyList.subList(1, length));
        System.out.println("standard diviation: " + standardDiviation);
        Map<String, Long> minMax = GetObjects.getMinMax(latencyList.subList(1, length));
        System.out.println("min: " + minMax.get("min") + ", max: " + minMax.get("max"));
        SaveStatisticUtils.saveResultToFile(latencyList.toString(), logFilename);
        String statisticValue = "\ntotal requests: " + latencyList.size() + ", average latency: " + averageLatency + ", standard diviation: " + standardDiviation + ", min: " + minMax.get("min") + ", max: " + minMax.get("max") + "\n\n";
        SaveStatisticUtils.saveResultToFile(statisticValue, logFilename);
        return averageLatency;

    }

    public static long uploadObjectsInParallel(S3Client s3Client, String bucketName, List<String> keyNameList, String content) {

        CountDownLatch countDownLatch = new CountDownLatch(keyNameList.size());

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < keyNameList.size(); i++) {
            executorService.execute(new UpdateTask(countDownLatch, s3Client, bucketName, keyNameList.get(i), content));
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //executorService.shutdown();
        long stop = System.currentTimeMillis();

        System.out.println("total latency: " + (stop - startTime));

        return (stop - startTime);
    }

}


class UpdateTask implements Runnable {
    private CountDownLatch countDownLatch;

    private S3Client s3Client;

    private String bucketName;

    private String key;

    private String content;

    public UpdateTask(CountDownLatch countDownLatch, S3Client s3Client, String bucketName, String key, String content) {
        this.countDownLatch = countDownLatch;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.content = content;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " is running");
        AWSS3Driver.putObjectFromString(s3Client, bucketName, key, content);
        countDownLatch.countDown();
    }
}
