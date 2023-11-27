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
 * @Description
 * @Date 2021/10/12 22:35
 * @Created by X1 Carbon
 */
public class GetBlocksFromSameObjectInParallel {

    private static int MB = 1024 * 1024;

    private static int KB = 1024;

    private static int OBJECT_SIZE = 256 * MB;

    private static String KEY = "T256MB-00";

    private static int MAX_THRESHOLDS_FOR_DOWNLOADS = 4;

    static ExecutorService executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);


    public static void main(String[] args) {
        testDownloadSmallBlocksInParallel();
    }

    public static void testDownloadNotSmallBlocksInParallel() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16);
        List<Integer> blockSizeList = Arrays.asList(4*MB, 8*MB, 16*MB);
        int times = 1;

        for (int i = 0; i < blockSizeList.size(); i++) {
            int blockSize = blockSizeList.get(i);

            for (int threadNum : threadNumList) {
                System.out.println("block size: " + blockSize / KB);
                System.out.println("block number: " + threadNum);

                MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
                executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

                String logName = String.format("parallel-block-read-%dKB-%d-thread.log", blockSize/KB, threadNum);
                double result = testDownloadBlocksInParallel(s3Client, bucketName, blockSize, threadNum, times, logName);
                System.out.println("average latency: " + result);
                executorService.shutdown();
            }
        }


    }

    public static void testDownloadSmallBlocksInParallel() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Integer> threadNumList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512);
        List<Integer> blockSizeList = Arrays.asList(1*KB, 8*KB, 64*KB, 512*KB);
        int times = 1;

        for (int i = 0; i < blockSizeList.size(); i++) {
            int blockSize = blockSizeList.get(i);

            for (int threadNum : threadNumList) {
                System.out.println("block size: " + blockSize / KB);
                System.out.println("block number: " + threadNum);
                if (blockSize == 512 * KB && threadNum == 512) {
                    continue;
                }

                MAX_THRESHOLDS_FOR_DOWNLOADS = threadNum;
                executorService = Executors.newFixedThreadPool(MAX_THRESHOLDS_FOR_DOWNLOADS);

                String logName = String.format("parallel-block-read-%dKB-%d-thread.log", blockSize/KB, threadNum);
                double result = testDownloadBlocksInParallel(s3Client, bucketName, blockSize, threadNum, times, logName);
                System.out.println("average latency: " + result);
                executorService.shutdown();
            }
        }


    }

    public static double testDownloadBlocksInParallel(S3Client s3Client, String bucketName, int blockSize, int blockNum, int times, String logFilename) {
        List<Long> latencyList = new ArrayList<>();
        SaveStatisticUtils.saveResultToFile("block size: " + blockSize + "; block number: " + blockNum + "\n", logFilename);
        for (int i = 0; i < times; i++) {
            System.out.println("-----------------------------------" + i);
            long latency = downloadBlocksInParallel(s3Client, bucketName, blockSize, blockNum);
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


    public static long downloadBlocksInParallel(S3Client s3Client, String bucketName, int blockSize, int blockNum) {

        List<Range> rangeList = new ArrayList<>();
        for (int i = 0; i < blockNum; i++) {
            Range range = new Range(i * blockSize, (i + 1) * blockSize - 1);
            rangeList.add(range);
        }

        CountDownLatch countDownLatch = new CountDownLatch(blockNum);

        long startTime = System.currentTimeMillis();
        for (Range range : rangeList) {
            executorService.execute(new DownloadBlockTask(countDownLatch, s3Client, bucketName, KEY, range.getStart(), range.getEnd()));
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long stop = System.currentTimeMillis();

        System.out.println("total latency: " + (stop - startTime));

        return (stop - startTime);
    }
}

class Range {
    private long start;

    private long end;

    public Range(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }
}

class DownloadBlockTask implements Runnable {
    private CountDownLatch countDownLatch;

    private S3Client s3Client;

    private String bucketName;

    private String key;

    private long start;

    private long end;

    public DownloadBlockTask(CountDownLatch countDownLatch, S3Client s3Client, String bucketName, String key, long start, long end) {
        this.countDownLatch = countDownLatch;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " is running to fetch key " + key + " with range [" + start/1024 + ", " + end/1024 + "]");
        AWSS3Driver.getObjectDataAsByteArrayWithRange(s3Client, bucketName, key, start, end);
        countDownLatch.countDown();
    }
}
