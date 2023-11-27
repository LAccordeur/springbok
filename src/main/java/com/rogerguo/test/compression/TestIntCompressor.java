package com.rogerguo.test.compression;

import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.util.ParseTrajectoryFromDataset;

import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-08-06 5:40 PM
 **/
public class TestIntCompressor {

    /*public static void main(String[] args) {
        testTrajectoryLatLongList();
    }

    public static long[] listToArray(List<Long> list) {
        long[] array = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    public static long[] intListToArray(List<Integer> list) {
        long[] array = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    public static void testTrajectoryMergedLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        List<Long> valueList =  LonLatTransformation.transfer2EncodedValues(trajectoryPointList);

        long[] input = listToArray(valueList);
        long[] compressed = new long[input.length];
        System.out.println(input.length);
        int amount = Simple8RLE.compress(input, compressed);
        System.out.println(amount);

    }

    public static void testTrajectoryLonLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Integer>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedValues(trajectoryPointList);

        List<Integer> lonValueList = valueLists.get("lon");

        long[] input = intListToArray(lonValueList);
        long[] compressed = new long[input.length];
        System.out.println(input.length);
        int amount = Simple8RLE.compress(input, compressed);
        System.out.println(amount);

    }

    public static void testTrajectoryLatLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Integer>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedValues(trajectoryPointList);

        List<Integer> latValueList = valueLists.get("lat");

        long[] input = intListToArray(latValueList);
        long[] compressed = new long[input.length];
        System.out.println(input.length);
        int amount = Simple8RLE.compress(input, compressed);
        System.out.println(amount);

    }

    public static void test() {
        long[] input = new long[]{1,2,3,4};
        long[] compressed = new long[input.length];

        int amount = Simple8.compress(input, compressed);
        System.out.println(amount);
    }*/

}
