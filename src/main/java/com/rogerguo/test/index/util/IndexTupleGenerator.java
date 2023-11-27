package com.rogerguo.test.index.util;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialTemporalBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.TrajectorySegmentMeta;

import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2021-09-13 1:02 PM
 **/
public class IndexTupleGenerator {

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            TrajectorySegmentMeta meta = generateNextSyntheticTupleForIndexTest("device_test", 1,10,5);
            System.out.println(meta);
        }
    }

    private static int count = 0;

    public static TrajectorySegmentMeta generateNextSyntheticTupleForIndexTest(String oid, double spatialBoxWidth, int timeWidth, int numOfMiddlePoints) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        long startTimestamp = count;
        long stopTimestamp = count + timeWidth;
        double startLongitude = count;
        double stopLongitude = count + spatialBoxWidth;
        double startLatitude = count;
        double stopLatitude = count + spatialBoxWidth;
        count++;

        int intervalForTimestamp = timeWidth / numOfMiddlePoints;
        double intervalForSpatial = spatialBoxWidth / numOfMiddlePoints;

        for (int i = 0; i < numOfMiddlePoints; i++) {
            long timestamp = startTimestamp + i * intervalForTimestamp;
            double longitude = startLongitude + i * intervalForSpatial;
            double latitude = startLatitude + i * intervalForSpatial;
            TrajectoryPoint point = new TrajectoryPoint(oid, timestamp, longitude, latitude);
            pointList.add(point);
        }
        TrajectoryPoint upperRightPoint = new TrajectoryPoint(oid, stopTimestamp, stopLongitude, stopLatitude);
        pointList.add(upperRightPoint);

        return new TrajectorySegmentMeta(startTimestamp, stopTimestamp, new Point(startLongitude, startLatitude), new Point(stopLongitude, stopLatitude), oid, oid+"-"+count, pointList);
    }

    /**
     * for easy to test, each trajectory only have two points which is also the points of bounding box
     * @param entryNum
     * @param spatialBoxWidth
     * @param timeWidth
     * @return
     */
    public static List<TrajectorySegmentMeta> generateSyntheticTupleForIndexTest(int entryNum, double spatialBoxWidth, long timeWidth) {
        List<TrajectorySegmentMeta> metaList = new ArrayList<>();

        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < entryNum; i++) {
            double longitude = i;
            double latitude = i;
            long time = i;
            String oid = String.valueOf(i);
            List<TrajectoryPoint> pointList = new ArrayList<>();
            pointList.add(new TrajectoryPoint(oid, time, longitude, latitude));
            pointList.add(new TrajectoryPoint(oid, time+timeWidth, longitude+spatialBoxWidth, latitude+spatialBoxWidth));
            TrajectorySegmentMeta trajectorySegmentMeta = new TrajectorySegmentMeta(time, time+timeWidth, new Point(longitude, latitude), new Point(longitude+spatialBoxWidth, latitude+spatialBoxWidth), oid, oid, pointList);
            metaList.add(trajectorySegmentMeta);
        }

        return metaList;
    }

    public static List<TrajectorySegmentMeta> generateRandomTupleForIndexTest(int pointNumPerEntry, int entryNum) {
        List<TrajectorySegmentMeta> resultList = new ArrayList<>();
        for (int i = 0; i < entryNum; i++) {
            String oid = String.valueOf(i);
            List<TrajectoryPoint> pointList = generateRandomTrajectoryPoints(oid, pointNumPerEntry);
            SpatialTemporalBoundingBox boundingBox = getBoundingBox(pointList);

            TrajectorySegmentMeta segmentMeta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            resultList.add(segmentMeta);
        }

        return resultList;
    }

    public static List<TrajectoryPoint> generateRandomTrajectoryPoints(String oid, int pointNum) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < pointNum; i++) {
            double longitude = random.nextDouble() * 360 - 180;
            double latitude = random.nextDouble() * 180 -90;
            TrajectoryPoint point = new TrajectoryPoint(oid, i, longitude, latitude);
            pointList.add(point);
        }

        return pointList;
    }

    public static SpatialTemporalBoundingBox getBoundingBox(List<TrajectoryPoint> pointList) {
        long timeStart = Long.MAX_VALUE;
        long timeStop = Long.MIN_VALUE;
        double lonMin = Double.MAX_VALUE;
        double lonMax = Double.MIN_VALUE;
        double latMin = Double.MAX_VALUE;
        double latMax = Double.MIN_VALUE;

        for (TrajectoryPoint point : pointList) {
            if (point.getTimestamp() < timeStart) {
                timeStart = point.getTimestamp();
            }
            if (point.getTimestamp() > timeStop) {
                timeStop = point.getTimestamp();
            }
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

        return new SpatialTemporalBoundingBox(new Point(lonMin, latMin), new Point(lonMax, latMax), timeStart, timeStop);
    }

}
