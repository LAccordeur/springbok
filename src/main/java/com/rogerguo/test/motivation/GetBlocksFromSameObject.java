package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.aws.AWSS3Driver;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.rogerguo.test.motivation.GetObjects.*;

/**
 * @Description
 * @Date 2021/10/12 21:21
 * @Created by X1 Carbon
 */
public class GetBlocksFromSameObject {

    private static int MB = 1024 * 1024;

    private static int KB = 1024;

    private static int OBJECT_SIZE = 256 * MB;

    private static String key = "T256MB-00";

    public static void main(String[] args) {

        testDownload8KBDataBlock();
        testDownload64KBDataBlock();
        testDownload512KBDataBlock();

    }

    public static void testDownload512KBDataBlock() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        int blockSize = 512 * KB;
        int requestNum = 500;

        downloadDataBlockAtBeginning(s3Client, bucketName, key, blockSize, requestNum, "read-512KB-block-beginning.log");
        downloadDataBlockAtMiddle(s3Client, bucketName, key, blockSize, requestNum, "read-512KB-block-middle.log");
        downloadDataBlockAtEnd(s3Client, bucketName, key, blockSize, requestNum, "read-512KB-block-end.log");

    }

    public static void testDownload64KBDataBlock() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        int blockSize = 64 * KB;
        int requestNum = 500;

        downloadDataBlockAtBeginning(s3Client, bucketName, key, blockSize, requestNum, "read-64KB-block-beginning.log");
        downloadDataBlockAtMiddle(s3Client, bucketName, key, blockSize, requestNum, "read-64KB-block-middle.log");
        downloadDataBlockAtEnd(s3Client, bucketName, key, blockSize, requestNum, "read-64KB-block-end.log");

    }

    public static void testDownload8KBDataBlock() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        int blockSize = 8 * KB;
        int requestNum = 500;

        downloadDataBlockAtBeginning(s3Client, bucketName, key, blockSize, requestNum, "read-8KB-block-beginning.log");
        downloadDataBlockAtMiddle(s3Client, bucketName, key, blockSize, requestNum, "read-8KB-block-middle.log");
        downloadDataBlockAtEnd(s3Client, bucketName, key, blockSize, requestNum, "read-8KB-block-end.log");

    }

    public static double downloadDataBlockAtBeginning(S3Client s3Client, String bucketName, String key, int blockSize, int requestNum, String logFilename) {


        SaveStatisticUtils.saveResultToFile(key + "\n", logFilename);
        List<Long> latencyList = new ArrayList<>();

        for (int i = 0; i < requestNum; i++) {
            System.out.println(i);
            long startTime = System.currentTimeMillis();
            byte[] result = AWSS3Driver.getObjectDataAsByteArrayWithRange(s3Client, bucketName, key, 0, blockSize-1);
            long stopTime = System.currentTimeMillis();
            System.out.println("result size: " + result.length);
            long latency = stopTime - startTime;
            latencyList.add(latency);
        }

        System.out.println(latencyList);
        int length = latencyList.size();
        double averageLatency = calculateAverage(latencyList.subList(1, length));
        double standardDiviation = calculateStandardDiviation(latencyList.subList(1, length));
        System.out.println("standard deviation: " + standardDiviation);
        Map<String, Long> minMax = getMinMax(latencyList.subList(1, length));
        System.out.println("min: " + minMax.get("min") + ", max: " + minMax.get("max"));
        SaveStatisticUtils.saveResultToFile(latencyList.toString(), logFilename);
        String statisticValue = "\ntotal requests: " + latencyList.size() + ", average latency: " + averageLatency + ", standard diviation: " + standardDiviation + ", min: " + minMax.get("min") + ", max: " + minMax.get("max") + "\n\n";
        SaveStatisticUtils.saveResultToFile(statisticValue, logFilename);
        return averageLatency;

    }


    public static double downloadDataBlockAtMiddle(S3Client s3Client, String bucketName, String key, int blockSize, int requestNum, String logFilename) {


        SaveStatisticUtils.saveResultToFile(key + "\n", logFilename);
        List<Long> latencyList = new ArrayList<>();

        for (int i = 0; i < requestNum; i++) {
            System.out.println(i);
            long startTime = System.currentTimeMillis();
            byte[] result = AWSS3Driver.getObjectDataAsByteArrayWithRange(s3Client, bucketName, key, OBJECT_SIZE/2, OBJECT_SIZE/2 + blockSize-1);
            long stopTime = System.currentTimeMillis();
            System.out.println("result size: " + result.length);
            long latency = stopTime - startTime;
            latencyList.add(latency);
        }

        System.out.println(latencyList);
        int length = latencyList.size();
        double averageLatency = calculateAverage(latencyList.subList(1, length));
        double standardDiviation = calculateStandardDiviation(latencyList.subList(1, length));
        System.out.println("standard deviation: " + standardDiviation);
        Map<String, Long> minMax = getMinMax(latencyList.subList(1, length));
        System.out.println("min: " + minMax.get("min") + ", max: " + minMax.get("max"));
        SaveStatisticUtils.saveResultToFile(latencyList.toString(), logFilename);
        String statisticValue = "\ntotal requests: " + latencyList.size() + ", average latency: " + averageLatency + ", standard diviation: " + standardDiviation + ", min: " + minMax.get("min") + ", max: " + minMax.get("max") + "\n\n";
        SaveStatisticUtils.saveResultToFile(statisticValue, logFilename);
        return averageLatency;

    }

    public static double downloadDataBlockAtEnd(S3Client s3Client, String bucketName, String key, int blockSize, int requestNum, String logFilename) {


        SaveStatisticUtils.saveResultToFile(key + "\n", logFilename);
        List<Long> latencyList = new ArrayList<>();

        for (int i = 0; i < requestNum; i++) {
            System.out.println(i);
            long startTime = System.currentTimeMillis();
            byte[] result = AWSS3Driver.getObjectDataAsByteArrayWithRange(s3Client, bucketName, key, OBJECT_SIZE-blockSize, OBJECT_SIZE - 1);
            long stopTime = System.currentTimeMillis();
            System.out.println("result size: " + result.length);
            long latency = stopTime - startTime;
            latencyList.add(latency);
        }

        System.out.println(latencyList);
        int length = latencyList.size();
        double averageLatency = calculateAverage(latencyList.subList(1, length));
        double standardDiviation = calculateStandardDiviation(latencyList.subList(1, length));
        System.out.println("standard deviation: " + standardDiviation);
        Map<String, Long> minMax = getMinMax(latencyList.subList(1, length));
        System.out.println("min: " + minMax.get("min") + ", max: " + minMax.get("max"));
        SaveStatisticUtils.saveResultToFile(latencyList.toString(), logFilename);
        String statisticValue = "\ntotal requests: " + latencyList.size() + ", average latency: " + averageLatency + ", standard diviation: " + standardDiviation + ", min: " + minMax.get("min") + ", max: " + minMax.get("max") + "\n\n";
        SaveStatisticUtils.saveResultToFile(statisticValue, logFilename);
        return averageLatency;

    }



}
