package com.rogerguo.test.util;

import com.rogerguo.test.common.TrajectoryPoint;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-06-18 11:46 AM
 **/
public class ParseTrajectoryFromDataset {

    public static void main(String[] args) {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = parseTrajectory(filePath, 6);
        System.out.println(trajectoryPointList.size());
    }

    public static Map<String, List<Double>> getValueList(List<TrajectoryPoint> pointList) {
        Map<String, List<Double>> resultMap = new HashMap<>();
        List<Double> longitudeList = new ArrayList<>();
        resultMap.put("lon", longitudeList);
        List<Double> latitudeList = new ArrayList<>();
        resultMap.put("lat", latitudeList);

        for (TrajectoryPoint point : pointList) {
            longitudeList.add(point.getLongitude());
            latitudeList.add(point.getLatitude());
        }

        return resultMap;
    }

    public static List<TrajectoryPoint> parseTrajectory(String filePath, int invalidedLineNum) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
            for (int i = 0; i < invalidedLineNum; i++) {
                reader.readLine();
            }

            String line = null;
            while ((line = reader.readLine()) != null) {
                String item[] = line.split(",");
                double latitude = Double.valueOf(item[0]);
                double longitude = Double.valueOf(item[1]);
                TrajectoryPoint trajectoryPoint = new TrajectoryPoint(null, 0L, longitude, latitude);
                pointList.add(trajectoryPoint);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return pointList;
    }

}
