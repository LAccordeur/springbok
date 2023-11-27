package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.aws.AWSS3Driver;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.util.*;

/**
 * @author yangguo
 * @create 2021-09-27 3:29 PM
 **/
public class GetObjects {

    public static void main(String[] args) {
        singleObjectGetTestBatchForSmallObject();
    }

    public static void test() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();


        String key = "T256MB-00";
        int requestNum = 500;

        double result = singleObjetGetTest(s3Client, bucketName, key, requestNum, "read-256mb.log");
        System.out.println("average latency: " + result);
    }

    public static void singleObjectGetTestBatchForSmallObject() {
        List<String> keyList = Arrays.asList("T1KB-00", "T2KB-00", "T4KB-00", "T8KB-00", "T16KB-00", "T32KB-00", "T64KB-00", "T128KB-00", "T256KB-00", "T512KB-00", "T1MB-00", "T2MB-00", "T4MB-00", "T8MB-00", "T16MB-00", "T32MB-00");

        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        int requestNum = 500;

        for (String key : keyList) {
            System.out.println(key);
            String logFileName = String.format("single-object-read-%s.log", key);
            singleObjetGetTest(s3Client, bucketName, key, requestNum, logFileName);
        }

    }

    public static void singleObjectGetTestBatchForNotSmallObject() {
        List<String> keyList = Arrays.asList("T64MB-00", "T128MB-00", "T256MB-00");

        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        int requestNum = 100;

        for (String key : keyList) {
            System.out.println(key);
            String logFileName = String.format("single-object-read-%s.log", key);
            singleObjetGetTest(s3Client, bucketName, key, requestNum, logFileName);
        }

    }

    public static double singleObjetGetTest(String key, int requestNum, String logFilename) {
        String bucketName = "bucket1614680484682";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<Long> latencyList = new ArrayList<>();

        for (int i = 0; i < requestNum; i++) {
            long startTime = System.currentTimeMillis();
            AWSS3Driver.getObjectDataAsByteArray(s3Client, bucketName, key);
            long stopTime = System.currentTimeMillis();
            long latency = stopTime - startTime;
            latencyList.add(latency);
        }

        System.out.println(latencyList);
        int length = latencyList.size();
        return calculateAverage(latencyList.subList(1, length));
    }

    public static double singleObjetGetTest(S3Client s3Client, String bucketName, String key, int requestNum, String logFilename) {
        SaveStatisticUtils.saveResultToFile(key + "\n", logFilename);
        List<Long> latencyList = new ArrayList<>();

        for (int i = 0; i < requestNum; i++) {
            System.out.println(i);
            long startTime = System.currentTimeMillis();
            AWSS3Driver.getObjectDataAsString(s3Client, bucketName, key);
            long stopTime = System.currentTimeMillis();
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

    public static Map<String, Long> getMinMax(List<Long> values) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long value : values) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }

        Map<String, Long> result = new HashMap<>();
        result.put("min", min);
        result.put("max", max);
        return result;
    }

    public static double calculateStandardDiviation(List<Long> values) {
        double average = calculateAverage(values);
        double var = 0;
        for (long value : values) {
            var = var + (value - average) * (value - average);
        }
        return Math.sqrt(var/values.size());
    }

    public static double calculateAverage(List<Long> values) {
        return values.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

}

