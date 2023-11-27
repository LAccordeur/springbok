package com.rogerguo.test.compression;

import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.util.ZCurve;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-06-18 6:09 PM
 **/
public class LonLatTransformation {

    private static ZCurve zCurve  = new ZCurve();

    public static List<Long> transfer2EncodedValues(List<TrajectoryPoint> pointList) {
        List<Long> resultList = new ArrayList<>();

        for (TrajectoryPoint point : pointList) {
            int normalizedLon = NormalizedDimensionHelper.normalizedLon(point.getLongitude());
            int normalizedLat = NormalizedDimensionHelper.normalizedLat(point.getLatitude());
            long encoded = zCurve.getCurveValue(normalizedLon, normalizedLat);
            resultList.add(encoded);
        }

        return resultList;
    }

    public static Map<String, List<Long>> transfer2SeperatedEncodedAndMergedValues(List<TrajectoryPoint> pointList) {
        Map<String, List<Integer>> phaseOneResult = transfer2SeperatedEncodedValues(pointList);
        List<Integer> lonIntList = phaseOneResult.get("lon");
        List<Integer> latIntList = phaseOneResult.get("lat");
        if (lonIntList.size() % 2 == 1) {
            lonIntList.add(lonIntList.get(lonIntList.size()-1));
        }
        if (latIntList.size() % 2 == 1) {
            latIntList.add(latIntList.get(latIntList.size())-1);
        }

        List<Long> resultLonList = new ArrayList<>();
        List<Long> resultLatList = new ArrayList<>();
        for (int i = 0; i < lonIntList.size(); i=i+2) {
            resultLonList.add(zCurve.getCurveValue(lonIntList.get(i), lonIntList.get(i+1)));
            resultLatList.add(zCurve.getCurveValue(latIntList.get(i), latIntList.get(i+1)));
        }

        Map<String, List<Long>> resultMap = new HashMap<>();
        resultMap.put("lon", resultLonList);
        resultMap.put("lat", resultLatList);

        return resultMap;
    }

    public static Map<String, List<Integer>> transfer2SeperatedEncodedValues(List<TrajectoryPoint> pointList) {
        Map<String, List<Integer>> resultMap = new HashMap<>();
        List<Integer> lonList = new ArrayList<>();
        List<Integer> latList = new ArrayList<>();
        resultMap.put("lon", lonList);
        resultMap.put("lat", latList);

        for (TrajectoryPoint point : pointList) {
            int normalizedLon = NormalizedDimensionHelper.normalizedLon(point.getLongitude());
            int normalizedLat = NormalizedDimensionHelper.normalizedLat(point.getLatitude());
            lonList.add(normalizedLon);
            latList.add(normalizedLat);
        }

        return resultMap;
    }
}
