package com.rogerguo.test.util;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.Chunk;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * generate trajectory for test use
 *
 * @author yangguo
 * @create 2021-06-11 5:36 PM
 **/
public class TrajectorySimulator {

    public static void main(String[] args) {
        for (int i = 0; i < 12; i++) {
            System.out.println(nextSyntheticTrajectoryPointBatch(2));
        }

    }

    static int batchNumBatch = 0;
    public static List<TrajectoryPoint> nextSyntheticTrajectoryPointBatch(int deviceNum) {
        List<TrajectoryPoint> trajectoryPointList = new ArrayList<>();
        for (int i = 0; i < deviceNum; i++) {
            String oid = "device_" + i;
            TrajectoryPoint trajectoryPoint = new TrajectoryPoint(oid, batchNumBatch, i + batchNumBatch, i + batchNumBatch);
            trajectoryPointList.add(trajectoryPoint);
        }
        batchNumBatch = batchNumBatch + 1;

        return trajectoryPointList;
    }

    static int num = 0;
    public static TrajectoryPoint nextSyntheticTrajectoryPoint(String deviceId) {
        TrajectoryPoint trajectoryPoint = new TrajectoryPoint(deviceId, num, num, num);
        num = num + 1;
        return trajectoryPoint;
    }

    /**
     * generate deviceNum * pointNum trajectory points, the points are first sorted by time and then by device_id
     * the location of each point is randomly generated
     * @param deviceNum
     * @param pointNum
     * @return
     */
    public static List<TrajectoryPoint> generateTrajectory(int deviceNum, int pointNum) {
        List<TrajectoryPoint> trajectoryPointList = new ArrayList<>();

        List<String> deviceIdList = deviceIdGenerator(deviceNum);
        Random random = new Random(System.currentTimeMillis());
        for (int timeIndex = 0; timeIndex < pointNum; timeIndex++) {
            for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
                TrajectoryPoint trajectoryPoint = new TrajectoryPoint(deviceIdList.get(deviceIndex), timeIndex, random.nextDouble(), random.nextDouble());
                trajectoryPointList.add(trajectoryPoint);
            }
        }

        return trajectoryPointList;
    }

    public static List<TrajectoryPoint> generateSyntheticTrajectory(int deviceNum, int pointNum) {
        List<TrajectoryPoint> trajectoryPointList = new ArrayList<>();

        List<String> deviceIdList = deviceIdGenerator(deviceNum);
        for (int timeIndex = 0; timeIndex < pointNum; timeIndex++) {
            for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
                TrajectoryPoint trajectoryPoint = new TrajectoryPoint(deviceIdList.get(deviceIndex), timeIndex, deviceIndex + timeIndex, deviceIndex + timeIndex);
                trajectoryPointList.add(trajectoryPoint);
            }
        }

        return trajectoryPointList;
    }

    public static SpatialBoundingBox generateSpatialBoundingBox(List<TrajectoryPoint> trajectoryPointList) {

        double lonMin = Double.MAX_VALUE;
        double lonMax = Double.MIN_VALUE;
        double latMin = Double.MAX_VALUE;
        double latMax = Double.MIN_VALUE;

        for (TrajectoryPoint point : trajectoryPointList) {
            if (point.getLongitude() < lonMin) {
                lonMin = point.getLongitude();
            }
            if (point.getLongitude() > lonMax) {
                lonMax = point.getLongitude();
            }
            if (point.getLatitude() < latMin) {
                latMin = point.getLatitude();
            }
            if (point.getLatitude() > latMax) {
                latMax = point.getLatitude();
            }
        }

        return new SpatialBoundingBox(new Point(lonMin, latMin), new Point(lonMax, latMax));
    }


    private static List<String> deviceIdGenerator(int deviceNum) {
        List<String> idList = new ArrayList<>();
        for (int i = 0; i < deviceNum; i++) {
            String deviceId = "device_" + i;
            idList.add(deviceId);
        }

        return idList;
    }


}
