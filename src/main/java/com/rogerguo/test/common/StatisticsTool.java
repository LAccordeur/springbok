package com.rogerguo.test.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2022-01-03 9:13 PM
 **/
public class StatisticsTool {

    public static Map<String, Double> calculateAverageAndStdDev(List<Integer> valueList) {
        Map<String, Double> result = new HashMap<>();

        if (valueList.size() == 0) {
            return result;
        }

        int total = 0;
        for (int value : valueList) {
            total = total + value;
        }
        int average = total / valueList.size();
        long squareSum = 0;
        for (int value : valueList) {
            squareSum = squareSum + (long) (value - average) * (value - average);
        }
        double stdDev = Math.sqrt(squareSum);

        result.put("avg", (double) average);
        result.put("stddev", stdDev);
        return result;
    }

    public static Map<String, Double> calculateAverageAndStdDevDouble(List<Double> valueList) {
        Map<String, Double> result = new HashMap<>();

        double total = 0;
        for (double value : valueList) {
            total = total + value;
        }
        double average = total / valueList.size();
        double squareSum = 0;
        for (double value : valueList) {
            squareSum = squareSum + (long) (value - average) * (value - average);
        }
        double stdDev = Math.sqrt(squareSum);

        result.put("avg", (double) average);
        result.put("stddev", stdDev);
        return result;
    }

}
