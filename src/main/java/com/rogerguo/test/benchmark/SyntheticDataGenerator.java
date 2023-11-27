package com.rogerguo.test.benchmark;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.PortoTaxiPoint;
import com.rogerguo.test.common.SpatialTemporalBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.util.IndexTupleGenerator;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.apache.commons.math3.random.UniformRandomGenerator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2021-12-14 3:05 PM
 **/
public class SyntheticDataGenerator {

    private static Random defaultRandom = new Random(2);

    public static List<TrajectoryPoint> generateFromTrajectoryGenerator(String filename) {
        List<TrajectoryPoint> pointList = new ArrayList<>();

        return pointList;
    }

    public static List<TrajectoryPoint> generateSyntheticPointsForStorageTest(int timeMax, int xMax, int yMax) {
        List<TrajectoryPoint> pointList = new ArrayList<>();

        for (int currentTimestamp = 0; currentTimestamp < timeMax; currentTimestamp++) {
            for (int x = 0; x < xMax; x++) {
                for (int y = 0; y < yMax; y++) {
                    String oid = String.valueOf(x*xMax+y);
                    TrajectoryPoint point = new TrajectoryPoint(oid, currentTimestamp, x, y);
                    pointList.add(point);
                }
            }
        }

        return pointList;
    }

    private static int movingObjectNum = 1000;
    private static int currentMovingObjectId = 0;
    private static int currentTimestamp = 0;
    private static Random random = new Random(1);
    private static List<Point> initPointList = new ArrayList<>();
    static {
        for (int i = 0; i < movingObjectNum; i++) {
            double x = random.nextDouble();
            double y = random.nextDouble();
            initPointList.add(new Point(x, y));
        }
    }

    public static TrajectoryPoint nextRandomTrajectoryPoint() {
        TrajectoryPoint trajectoryPoint = null;

        if (currentMovingObjectId >= movingObjectNum) {
            currentMovingObjectId = 0;
            currentTimestamp++;
            double lon = initPointList.get(currentMovingObjectId).getLongitude();
            double lat = initPointList.get(currentMovingObjectId).getLatitude();
            trajectoryPoint = new TrajectoryPoint(String.valueOf(currentMovingObjectId), currentTimestamp, lon, lat);
            currentMovingObjectId++;
        } else {
            double lon = initPointList.get(currentMovingObjectId).getLongitude();
            double lat = initPointList.get(currentMovingObjectId).getLatitude();
            trajectoryPoint = new TrajectoryPoint(String.valueOf(currentMovingObjectId), currentTimestamp, lon, lat);
            currentMovingObjectId++;
        }

        return trajectoryPoint;
    }

    public static List<TrajectoryPoint> generateRandomDistributedDataset(long listSize, double lonLimit, double latLimit) {
        System.out.println("generating random data set...\n");
        List<TrajectoryPoint> pointList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < listSize; i++) {
            double lon = random.nextDouble() * lonLimit;
            double lat = random.nextDouble() * latLimit;
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, lon, lat);
            pointList.add(point);
        }

        //saveToFile(pointList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/random-data-10w.txt");

        return pointList;
    }

    public static List<TrajectorySegmentMeta> generateRandomDistributedIndexEntries(long listSize, double lonLimit, double latLimit) {
        System.out.println("generating random index entries");
        List<TrajectorySegmentMeta> metaList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < listSize; i++) {
            String oid = String.valueOf(i);
            double centerLon = random.nextDouble() * lonLimit;
            double centerLat = random.nextDouble() * latLimit;
            int timestamp = i;
            List<TrajectoryPoint> pointList = generateLinePointsFromCenter(oid, centerLon, centerLat, timestamp, 0.01);
            SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(pointList);
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            metaList.add(meta);
        }

        return metaList;
    }

    private static List<TrajectoryPoint> generateRandomPointsAroundCenter(String oid, double centerLon, double centerLat, long timestamp, double factor) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int pointNum = 50;

        TrajectoryPoint centerPoint = new TrajectoryPoint(oid, timestamp, centerLon, centerLat);
        pointList.add(centerPoint);
        for (int i = 1; i < pointNum; i++) {
            double lon = centerLon + defaultRandom.nextDouble() * factor;
            double lat = centerLat + defaultRandom.nextDouble() * factor;
            long timestampCur = timestamp + i;
            TrajectoryPoint point = new TrajectoryPoint(oid, timestampCur, lon, lat);
            pointList.add(point);
        }

        return pointList;
    }

    private static List<TrajectoryPoint> generateLinePointsFromCenter(String oid, double centerLon, double centerLat, long timestamp, double factor) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int pointNum = 50;

        TrajectoryPoint centerPoint = new TrajectoryPoint(oid, timestamp, centerLon, centerLat);
        pointList.add(centerPoint);
        for (int i = 1; i < pointNum; i++) {
            double lon = centerLon + (factor / pointNum) * i;
            double lat = centerLat + defaultRandom.nextDouble() * (factor / pointNum) * i;
            long timestampCur = timestamp + i;
            TrajectoryPoint point = new TrajectoryPoint(oid, timestampCur, lon, lat);
            pointList.add(point);
        }

        return pointList;
    }

    private static void saveToFile(List<TrajectoryPoint> pointList, String filename) {
        File file = new File(filename);
        try {
            if (file.exists()) {
                file.delete();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file, true);
            for (TrajectoryPoint point : pointList) {
                String record = point.getOid() + "," + point.getLongitude() + "," + point.getLatitude() + "," + point.getTimestamp() + "\n";
                fileWriter.append(record);
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void saveToFileIndexEntries(List<TrajectorySegmentMeta> metaList, String filename) {
        File file = new File(filename);
        try {
            if (file.exists()) {
                file.delete();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file, true);
            for (TrajectorySegmentMeta meta : metaList) {
                double lon = (meta.getLowerLeft().getLongitude() + meta.getUpperRight().getLongitude()) / 2;
                double lat = (meta.getLowerLeft().getLatitude() + meta.getUpperRight().getLatitude()) / 2;
                String record = meta.getDeviceId() + "," + lon + "," + lat + "\n";
                fileWriter.append(record);
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    public static List<TrajectoryPoint> generateZipfDistributionDataset(long listSize, double lonLimit, double latLimit) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int max = 1000000;

        ZipfDistribution zipfDistribution = new ZipfDistribution(RandomGeneratorFactory.createRandomGenerator(new Random(1)), max, 0.8);
        Point center = new Point(lonLimit/2, latLimit/2) ;

        for (int i = 0; i < listSize; i++) {
            //int sample = zipfDistribution.sample();
            double lon = (zipfDistribution.sample() / (1.0 * max)) * lonLimit  + center.getLongitude();
            double lat = (zipfDistribution.sample() / (1.0 * max)) * latLimit  + center.getLatitude();
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), 0, lon, lat);
            pointList.add(point);
        }

        saveToFile(pointList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/zipf-data.txt");

        return pointList;
    }

    /**
     * in region lon: (0, lonLimit), (0, latLimit), generate 5 hotspots with gaussian distribution
     * @param listSize
     * @param lonLimit
     * @param latLimit
     * @return
     */
    public static List<TrajectoryPoint> generateGaussianDistributionDataSet(long listSize, double lonLimit, double latLimit) {
        List<TrajectoryPoint> pointList = new ArrayList<>();

        Random random = new Random(1);
        Point center = new Point(lonLimit/2, latLimit/1.5);  // 50%
        Point center1 = new Point(lonLimit/4, latLimit/1.8);   // 20%
        Point center2 = new Point(lonLimit/1.2, latLimit/4);   // 20%
        Point center3 = new Point(lonLimit/8, latLimit/6);      // 5%
        Point center4 = new Point(lonLimit/1.1, latLimit/1.4);  // 5%

        double factor = 10;

        if (listSize >= 1000000) {
            factor = 12;
        }

        if (listSize >= 10000000) {
            factor = 14;
        }

        for (int i = 0; i < listSize * 0.5; i++) {
            double lon = random.nextGaussian() / factor + center.getLongitude();
            double lat = random.nextGaussian() / factor + center.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center.getLongitude();
                lat = random.nextGaussian() / factor + center.getLatitude();
            }
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, lon, lat);
            pointList.add(point);
        }

        for (int i = (int)(listSize * 0.5); i < listSize * 0.7; i++) {
            double lon = random.nextGaussian() / factor + center1.getLongitude();
            double lat = random.nextGaussian() / factor + center1.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center1.getLongitude();
                lat = random.nextGaussian() / factor + center1.getLatitude();
            }
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, lon, lat);
            pointList.add(point);
        }

        for (int i = (int)(listSize * 0.7); i < listSize * 0.9; i++) {
            double lon = random.nextGaussian() / factor + center2.getLongitude();
            double lat = random.nextGaussian() / factor + center2.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center2.getLongitude();
                lat = random.nextGaussian() / factor + center2.getLatitude();
            }
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, lon, lat);
            pointList.add(point);
        }

        for (int i = (int)(listSize * 0.9); i < listSize * 0.95; i++) {
            double lon = random.nextGaussian() / factor + center3.getLongitude();
            double lat = random.nextGaussian() / factor + center3.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center3.getLongitude();
                lat = random.nextGaussian() / factor + center3.getLatitude();
            }
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, lon, lat);
            pointList.add(point);
        }

        for (int i = (int)(listSize * 0.95); i < listSize; i++) {
            double lon = random.nextGaussian() / factor + center4.getLongitude();
            double lat = random.nextGaussian() / factor + center4.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center4.getLongitude();
                lat = random.nextGaussian() / factor + center4.getLatitude();
            }
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, lon, lat);
            pointList.add(point);
        }

        //saveToFile(pointList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/gaussian-data-factor-1000w.txt");

        return pointList;

    }

    public static List<TrajectorySegmentMeta> generateGaussianDistributedIndexEntries(long listSize, double lonLimit, double latLimit) {
        System.out.println("generating gaussian index entries");
        List<TrajectorySegmentMeta> metaList = new ArrayList<>();
        Random random = new Random(1);

        Random randomTime = new Random(2);
        List<Integer> timeList = new ArrayList<>();
        for (int i = 0; i < listSize; i++) {
            int value = (int) Math.floor(Math.abs(randomTime.nextGaussian()) * listSize);
            while (value > 1000) {
                value = (int) Math.floor(Math.abs(randomTime.nextGaussian()) * listSize);
            }
            timeList.add(value);
        }
        Collections.sort(timeList);

        Point center = new Point(lonLimit/2, latLimit/1.5);  // 50%
        Point center1 = new Point(lonLimit/4, latLimit/1.8);   // 20%
        Point center2 = new Point(lonLimit/1.2, latLimit/4);   // 20%
        Point center3 = new Point(lonLimit/8, latLimit/6);      // 5%
        Point center4 = new Point(lonLimit/1.1, latLimit/1.4);  // 5%

        double factor = 10;

        if (listSize >= 1000000) {
            factor = 12;
        }

        if (listSize >= 10000000) {
            factor = 14;
        }

        for (int i = 0; i < listSize * 0.5; i++) {
            String oid = String.valueOf(i);
            double lon = random.nextGaussian() / factor + center.getLongitude();
            double lat = random.nextGaussian() / factor + center.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center.getLongitude();
                lat = random.nextGaussian() / factor + center.getLatitude();
            }
            int timestamp = timeList.get(i);
            List<TrajectoryPoint> pointList = generateLinePointsFromCenter(oid, lon, lat, timestamp, 0.01);
            SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(pointList);
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            metaList.add(meta);
        }

        for (int i = (int)(listSize * 0.5); i < listSize * 0.7; i++) {
            String oid = String.valueOf(i);
            double lon = random.nextGaussian() / factor + center1.getLongitude();
            double lat = random.nextGaussian() / factor + center1.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center1.getLongitude();
                lat = random.nextGaussian() / factor + center1.getLatitude();
            }
            int timestamp = timeList.get(i);
            List<TrajectoryPoint> pointList = generateLinePointsFromCenter(oid, lon, lat, timestamp, 0.01);
            SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(pointList);
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            metaList.add(meta);
        }

        for (int i = (int)(listSize * 0.7); i < listSize * 0.9; i++) {
            String oid = String.valueOf(i);
            double lon = random.nextGaussian() / factor + center2.getLongitude();
            double lat = random.nextGaussian() / factor + center2.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center2.getLongitude();
                lat = random.nextGaussian() / factor + center2.getLatitude();
            }
            int timestamp = timeList.get(i);
            List<TrajectoryPoint> pointList = generateLinePointsFromCenter(oid, lon, lat, timestamp, 0.01);
            SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(pointList);
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            metaList.add(meta);
        }

        for (int i = (int)(listSize * 0.9); i < listSize * 0.95; i++) {
            String oid = String.valueOf(i);
            double lon = random.nextGaussian() / factor + center3.getLongitude();
            double lat = random.nextGaussian() / factor + center3.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center3.getLongitude();
                lat = random.nextGaussian() / factor + center3.getLatitude();
            }
            int timestamp = timeList.get(i);
            List<TrajectoryPoint> pointList = generateLinePointsFromCenter(oid, lon, lat, timestamp, 0.01);
            SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(pointList);
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            metaList.add(meta);
        }

        for (int i = (int)(listSize * 0.95); i < listSize; i++) {
            String oid = String.valueOf(i);
            double lon = random.nextGaussian() / factor + center4.getLongitude();
            double lat = random.nextGaussian() / factor + center4.getLatitude();
            while (!(lon > 0 && lon < lonLimit && lat > 0 && lat < latLimit)) {
                lon = random.nextGaussian() / factor + center4.getLongitude();
                lat = random.nextGaussian() / factor + center4.getLatitude();
            }
            int timestamp = timeList.get(i);
            List<TrajectoryPoint> pointList = generateLinePointsFromCenter(oid, lon, lat, timestamp, 0.01);
            SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(pointList);
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(boundingBox.getStartTime(), boundingBox.getStopTime(), boundingBox.getLowerLeft(), boundingBox.getUpperRight(), oid, oid, pointList);
            metaList.add(meta);
        }

        //saveToFileIndexEntries(metaList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/gaussian-index-1w.txt");

        return metaList;
    }

    public static List<TrajectoryPoint> gaussian() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < 1000000; i++) {
            double lon = random.nextGaussian() / 1000.0 + 3;
            double lat = random.nextGaussian() / 1000.0 + 3;
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), 0, lon, lat);
            pointList.add(point);
        }
        saveToFile(pointList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/gaussian-data-test.txt");


        return pointList;
    }

    @Deprecated
    public static List<TrajectoryPoint> zipf() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int max = 1000000;

        ZipfDistribution zipfDistribution = new ZipfDistribution(RandomGeneratorFactory.createRandomGenerator(new Random(1)), max, 0.8);

        for (int i = 0; i < 10000; i++) {
            //int sample = zipfDistribution.sample();
            double lon = zipfDistribution.sample();
            double lat = zipfDistribution.sample();
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), 0, lon, lat);
            pointList.add(point);
        }

        saveToFile(pointList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/zipf-data-test.txt");

        return pointList;
    }

    public static List<TrajectoryPoint> normalDistribution() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        NormalDistribution normalDistribution = new NormalDistribution(RandomGeneratorFactory.createRandomGenerator(new Random(1)), 0, 20);

        for (int i = 0; i < 10000; i++) {
            double lon = normalDistribution.sample();
            double lat = normalDistribution.sample();
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), 0, lon, lat);
            pointList.add(point);
        }
        saveToFile(pointList, "/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/dataset/normal-data-test-20.txt");


        return pointList;

    }

    public static void main(String[] args) {


        /*ZipfDistribution zipfDistribution = new ZipfDistribution(RandomGeneratorFactory.createRandomGenerator(new Random(1)),10, 1);
        for (int i = 0; i < 10; i++) {
            System.out.println(zipfDistribution.sample());
        }*/

        /*Random random = new Random(1);
        for (int i = 0; i < 20; i++) {
            System.out.println(random.nextGaussian());
        }*/


        //System.out.println(generateGaussianDistributionDataSet(10000, 10, 10));
        //System.out.println(generateRandomDistributedDataset(10000, 10, 10));
        //System.out.println(generateRandomPointsAroundCenter("1", 0.5, 0.5, 2, 0.1));
        //System.out.println(generateRandomDistributedIndexEntries(2, 1, 1));

        /*Random random = new Random(1);
        List<Integer> integerList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int value = (int) Math.floor(Math.abs(random.nextGaussian()) * 1000);
            while (value > 1000) {
                value = (int) Math.floor(Math.abs(random.nextGaussian()) * 1000);
            }
            integerList.add(value);
        }
        Collections.sort(integerList);
        System.out.println(integerList);*/

        /*List<TrajectoryPoint> result = generateSyntheticPointsForStorageTest(10, 2, 2);
        for (TrajectoryPoint point : result) {
            System.out.println(point);
        }*/
        //generateGaussianDistributionDataSet(10000000, 1, 1);

        //generateRandomDistributedDataset(100000, 10, 10);

        //gaussian();
        generateGaussianDistributedIndexEntries(10000, 1, 1);
    }

}


