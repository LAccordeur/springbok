package com.rogerguo.test.motivation;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yangguo
 * @create 2021-10-11 7:53 PM
 **/
public class GetFixedSizeDataInParallel {

    private static int KB = 1024;

    private static int MB = 1024 * 1024;

    private static int MAX_THRESHOLDS_FOR_DOWNLOADS = 4;

    static ExecutorService executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

    public static void main(String[] args) {
        testDownload128MBDataInParallel();
    }

    public static void testDownload128MBDataInParallel() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024);
        List<String> keyPrefixList = Arrays.asList("P128MB-", "P64MB-", "P32MB-", "P16MB-", "P8MB-", "P4MB-", "P2MB-", "P1MB-", "P512KB-", "P256KB-", "P128KB-");

        for (int i = 0; i < threadNumList.size(); i++) {
            int threadNum = threadNumList.get(i);
            String keyPrefix = keyPrefixList.get(i);

            MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
            executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

            List<String> keyList = new ArrayList<>();
            for (int j = 0; j < threadNum; j++) {
                String key = keyPrefix + String.format("%02d", j);
                keyList.add(key);
            }
            String logName = String.format("fixed-128MB-parallel-read-%s-%d-thread.log", keyPrefix, threadNum);
            testDownloadObjectsInParallel(s3Client, bucketName, keyList, 100, logName);
            executorService.shutdown();
        }


    }

    public static void testDownload8MBDataInParallel() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024);
        List<String> keyPrefixList = Arrays.asList("P8MB-", "P4MB-", "P2MB-", "P1MB-", "P512KB-", "P256KB-", "P128KB-", "P64KB-", "P32KB-", "P16KB-", "P8KB-");

        for (int i = 0; i < threadNumList.size(); i++) {
            int threadNum = threadNumList.get(i);
            String keyPrefix = keyPrefixList.get(i);

            MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
            executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

            List<String> keyList = new ArrayList<>();
            for (int j = 0; j < threadNum; j++) {
                String key = keyPrefix + String.format("%02d", j);
                keyList.add(key);
            }
            String logName = String.format("fixed-8MB-parallel-read-%s-%d-thread.log", keyPrefix, threadNum);
            testDownloadObjectsInParallel(s3Client, bucketName, keyList, 500, logName);
            executorService.shutdown();
        }


    }

    public static void testDownload32MBDataInParallel() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024);
        List<String> keyPrefixList = Arrays.asList("P32MB-", "P16MB-", "P8MB-", "P4MB-", "P2MB-", "P1MB-", "P512KB-", "P256KB-", "P128KB-", "P64KB-", "P32KB-");

        for (int i = 0; i < threadNumList.size(); i++) {
            int threadNum = threadNumList.get(i);
            String keyPrefix = keyPrefixList.get(i);

            MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
            executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

            List<String> keyList = new ArrayList<>();
            for (int j = 0; j < threadNum; j++) {
                String key = keyPrefix + String.format("%02d", j);
                keyList.add(key);
            }
            String logName = String.format("fixed-32MB-parallel-read-%s-%d-thread.log", keyPrefix, threadNum);
            testDownloadObjectsInParallel(s3Client, bucketName, keyList, 500, logName);
            executorService.shutdown();
        }


    }

    public static double testDownloadObjectsInParallel(S3Client s3Client, String bucketName, List<String> keyNameList, int times, String logFilename) {
        List<Long> latencyList = new ArrayList<>();
        SaveStatisticUtils.saveResultToFile(keyNameList + "\n", logFilename);
        for (int i = 0; i < times; i++) {
            System.out.println("-----------------------------------" + i);
            long latency = downloadObjectsInParallel(s3Client, bucketName, keyNameList);
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

    public static long downloadObjectsInParallel(S3Client s3Client, String bucketName, List<String> keyNameList) {
        //GetObjects.singleObjetGetTest(s3Client, bucketName, "T1KB-", 1, ""); // warm up

        //ExecutorService executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);
        CountDownLatch countDownLatch = new CountDownLatch(keyNameList.size());

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < keyNameList.size(); i++) {
            executorService.execute(new DownloadTask(countDownLatch, s3Client, bucketName, keyNameList.get(i)));
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
