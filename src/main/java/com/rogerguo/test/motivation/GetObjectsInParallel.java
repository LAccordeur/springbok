package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.aws.AWSS3Driver;
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
 * @create 2021-09-28 12:32 PM
 **/
public class GetObjectsInParallel {


    private static int MAX_THRESHOLDS_FOR_DOWNLOADS = 4;

    static ExecutorService executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

    public static void main(String[] args) {
        //testParallelReadBatchForSmallObject();
        testParallelReadBatchForNotSmallObject();
    }

    public static void test() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<String> keyList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String key = "P32MB-" + String.format("%02d", i);
            keyList.add(key);
        }

        double result = testDownloadObjectsInParallel(s3Client, bucketName, keyList, 500, "parallel-read-32mb-4thread.log");
        System.out.println("average latency: " + result);
        executorService.shutdown();
    }

    public static void testParallelReadBatchForNotSmallObject() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64);
        List<String> keyPrefixList = Arrays.asList("P4MB-", "P8MB-", "P16MB-", "P32MB-");

        for (String keyPrefix : keyPrefixList) {
            for (int threadNum : threadNumList) {
                MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
                executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

                List<String> keyList = new ArrayList<>();
                for (int i = 0; i < threadNum; i++) {
                    String key = keyPrefix + String.format("%02d",i);
                    keyList.add(key);
                }
                String logName = String.format("parallel-read-%s-%d-thread.log", keyPrefix, threadNum);
                double result = testDownloadObjectsInParallel(s3Client, bucketName, keyList, 1, logName);
                System.out.println("average latency: " + result);
                executorService.shutdown();
            }
        }

    }

    public static void testParallelReadBatchForSmallObject() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512);
        List<String> keyPrefixList = Arrays.asList("P1KB-", "P8KB-", "P64KB-", "P512KB-");

        for (String keyPrefix : keyPrefixList) {
            for (int threadNum : threadNumList) {
                MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
                executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

                List<String> keyList = new ArrayList<>();
                for (int i = 0; i < threadNum; i++) {
                    String key = keyPrefix + String.format("%02d",i);
                    keyList.add(key);
                }
                String logName = String.format("parallel-read-%s-%d-thread.log", keyPrefix, threadNum);
                double result = testDownloadObjectsInParallel(s3Client, bucketName, keyList, 10, logName);
                System.out.println("average latency: " + result);
                executorService.shutdown();
            }
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

class DownloadTask implements Runnable {
    private CountDownLatch countDownLatch;

    private S3Client s3Client;

    private String bucketName;

    private String key;

    public DownloadTask(CountDownLatch countDownLatch, S3Client s3Client, String bucketName, String key) {
        this.countDownLatch = countDownLatch;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " is running to fetch key " + key);
        AWSS3Driver.getObjectDataAsByteArray(s3Client, bucketName, key);
        countDownLatch.countDown();
    }
}
