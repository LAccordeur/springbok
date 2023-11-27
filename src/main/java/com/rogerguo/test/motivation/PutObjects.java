package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.aws.AWSS3Driver;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.*;

import static com.rogerguo.test.motivation.GetObjects.*;

/**
 * @author yangguo
 * @create 2021-09-27 2:47 PM
 **/
public class PutObjects {

    private static int KB = 1024;

    private static int MB = 1024 * 1024;

    public static void main(String[] args) {
        singleObjectPutSmallObjectTestBatch();
    }

    public static void singleObjectPutSmallObjectTestBatch() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<String> keyList = Arrays.asList("T1KB-00", "T2KB-00", "T4KB-00", "T8KB-00", "T16KB-00", "T32KB-00", "T64KB-00", "T128KB-00", "T256KB-00", "T512KB-00", "T1MB-00", "T2MB-00", "T4MB-00", "T8MB-00", "T16MB-00");
        Map<String, Integer> objectSizeMap = new HashMap<>();
        objectSizeMap.put("T1KB-00", 1*KB);
        objectSizeMap.put("T2KB-00", 2*KB);
        objectSizeMap.put("T4KB-00", 4*KB);
        objectSizeMap.put("T8KB-00", 8*KB);
        objectSizeMap.put("T16KB-00", 16*KB);
        objectSizeMap.put("T32KB-00", 32*KB);
        objectSizeMap.put("T64KB-00", 64*KB);
        objectSizeMap.put("T128KB-00", 128*KB);
        objectSizeMap.put("T256KB-00", 256*KB);
        objectSizeMap.put("T512KB-00", 512*KB);
        objectSizeMap.put("T1MB-00", 1*MB);
        objectSizeMap.put("T2MB-00", 2*MB);
        objectSizeMap.put("T4MB-00", 4*MB);
        objectSizeMap.put("T8MB-00", 8*MB);
        objectSizeMap.put("T16MB-00", 16*MB);



        int requestNum = 500;
        for (String key : keyList) {
            System.out.println(key);
            String logFileName = String.format("single-object-put-%s.log", key);
            int objectSize = objectSizeMap.get(key);
            singleObjetPutTest(s3Client, bucketName, key, objectSize, requestNum, logFileName);
        }
    }

    public static void singleObjectPutNotSmallTestBatch() {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        List<String> keyList = Arrays.asList("32MB-00", "64MB-00", "T128MB-00", "T256MB-00");
        Map<String, Integer> objectSizeMap = new HashMap<>();

        objectSizeMap.put("T32MB-00", 32*MB);
        objectSizeMap.put("T64MB-00", 64*MB);
        objectSizeMap.put("T128MB-00", 128*MB);
        objectSizeMap.put("T256MB-00", 256*MB);


        int requestNum = 100;
        for (String key : keyList) {
            System.out.println(key);
            String logFileName = String.format("single-object-put-%s.log", key);
            int objectSize = objectSizeMap.get(key);
            singleObjetPutTest(s3Client, bucketName, key, objectSize, requestNum, logFileName);
        }
    }

    public static double singleObjetPutTest(S3Client s3Client, String bucketName, String key, int objectSize, int requestNum, String logFilename) {
        SaveStatisticUtils.saveResultToFile(key + "\n", logFilename);
        List<Long> latencyList = new ArrayList<>();

        String content = generateRandomString(objectSize);
        for (int i = 0; i < requestNum; i++) {
            System.out.println(i);
            long startTime = System.currentTimeMillis();
            AWSS3Driver.putObjectFromString(s3Client, bucketName, key, content);
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

    public static String generateRandomString(int length) {
        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {

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

}
