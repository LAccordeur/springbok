package com.rogerguo.test.motivation;

import com.rogerguo.test.storage.aws.AWSS3Driver;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.rogerguo.test.motivation.GetObjects.*;

/**
 * @author yangguo
 * @create 2022-05-16 3:54 PM
 **/
public class ListObjects {

    public static void main(String[] args) {
        String bucketName = "motivation-test-1111";
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        //initBucket(s3Client, bucketName, "test1", 10);
        //listObjectsByPrefix(s3Client, bucketName, "test1", 10);

        //initBucket(s3Client, bucketName, "abc",10);
        listObjectsByPrefix(s3Client, bucketName,"abc", 10);
    }

    public static void listObjectsByPrefix(S3Client s3Client, String bucketName, String keyPrefix, int requestNum) {
        List<Long> latencyList = new ArrayList<>();

        for (int i = 0; i < requestNum; i++) {
            long startTime = System.currentTimeMillis();
            AWSS3Driver.listObjectKeysWithSamePrefix(s3Client, bucketName, keyPrefix);
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
        //SaveStatisticUtils.saveResultToFile(latencyList.toString(), logFilename);
        String statisticValue = "\ntotal requests: " + latencyList.size() + ", average latency: " + averageLatency + ", standard diviation: " + standardDiviation + ", min: " + minMax.get("min") + ", max: " + minMax.get("max") + "\n\n";
        //SaveStatisticUtils.saveResultToFile(statisticValue, logFilename);
        System.out.println(statisticValue);
    }

    private static void initBucket(S3Client s3Client, String bucketName, String keyPrefix, int objectNum) {

        for (int i = 0; i < objectNum; i++) {
            System.out.println(i);
            String objectKey = String.format("%s-%06d", keyPrefix, i);
            String objectData = generateRandomString(64);
            AWSS3Driver.putObjectFromString(s3Client, bucketName, objectKey, objectData);
        }

    }


    private static String generateRandomString(int length) {
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
