package com.rogerguo.test.benchmark.headchunk.semi;

import com.rogerguo.test.benchmark.MemoryUsageUtils;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.*;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author yangguo
 * @create 2021-12-24 9:06 PM
 **/
public class Test {

    public static void main(String[] args) {
        testGeoHashMemory();
    }

    public static void testPhyDeletion() {
        int listSize = 10000000;
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
        }

        List<Chunk> deletionList = new ArrayList<>();
        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(16, 100, seriesStore);

        for (int i = 0; i < pointList.size(); i++) {
            System.out.println(i);
            TrajectoryPoint point = pointList.get(i);
            indexWithGeoHashPhysicalSplit.updateIndex(point);

            if (deletionList.size() < 2000000) {
                Chunk chunk = new Chunk(String.valueOf(i));
                List<TrajectoryPoint> pointList1 = new ArrayList<>();
                pointList1.add(point);
                chunk.setChunk(pointList1);
                deletionList.add(chunk);
            }
        }
    }

    public static void baselineMem() {
        long listSize = 10000000;

        SeriesStore seriesStore;

        seriesStore = SeriesStore.initNewStoreForInMemTest();

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);


        long beforeMemory = MemoryUsageUtils.getUsedMemory();
        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
        }
        long afterMemory = MemoryUsageUtils.getUsedMemory();
        System.out.println("used memory: " + ((afterMemory - beforeMemory) / (1024 * 1024)) + " MB");
    }

    public static void testGeoHashMemory() {
        long listSize = 10000000;

        int shiftLength = 32;

        SeriesStore seriesStore;

        HeadChunkIndexWithGeoHash indexWithGeoHash = new HeadChunkIndexWithGeoHash(shiftLength);
        seriesStore = SeriesStore.initNewStoreForInMemTest();

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);

        long beforeMemory = MemoryUsageUtils.getUsedMemory();
        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
            indexWithGeoHash.updateIndex(point);
        }
        long afterMemory = MemoryUsageUtils.getUsedMemory();
        System.out.println("used memory: " + ((afterMemory - beforeMemory) / (1024 * 1024)) + " MB");
        System.out.println(indexWithGeoHash.printStatus());
    }

    public static void testSemiMemory() {
        long listSize = 10000000;

        int shiftLength = 16;

        int postingListCapacity = 100;

        HeadChunkIndexWithGeoHashSemiSplit indexWithGeoHashSemiSplit;

        SeriesStore seriesStore;

        indexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit(shiftLength, postingListCapacity);
        seriesStore = SeriesStore.initNewStoreForInMemTest();

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);

        long beforeMemory = MemoryUsageUtils.getUsedMemory();
        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
            indexWithGeoHashSemiSplit.updateIndex(point);
        }
        long afterMemory = MemoryUsageUtils.getUsedMemory();
        System.out.println("used memory: " + ((afterMemory - beforeMemory) / (1024 * 1024)) + " MB");
        System.out.println(indexWithGeoHashSemiSplit.printStatus());
    }

    public static void testRStarMemory() {
        long listSize = 10000000;

        HeadChunkIndexWithRStartreeMBR indexWithRStartreeMBR;

        SeriesStore seriesStore;

        indexWithRStartreeMBR = new HeadChunkIndexWithRStartreeMBR();
        seriesStore = SeriesStore.initNewStoreForInMemTest();

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);

        long beforeMemory = MemoryUsageUtils.getUsedMemory();
        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
            indexWithRStartreeMBR.updateIndex(point);
        }
        long afterMemory = MemoryUsageUtils.getUsedMemory();
        System.out.println("used memory: " + ((afterMemory - beforeMemory) / (1024 * 1024)) + " MB");
        System.out.println(indexWithRStartreeMBR.printStatus());
    }

    public static void testSemi() {
        long listSize = 10000000;

        int querySetSize = 500;

        double queryRegion = 0.1;

        int shiftLength = 8;

        int postingListCapacity = 25;

        List<SpatialBoundingBox> queryPredicateList;

        HeadChunkIndexWithGeoHashSemiSplit indexWithGeoHashSemiSplit;

        SeriesStore seriesStore;

        queryPredicateList = new ArrayList<>();
        indexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit(shiftLength, postingListCapacity);
        seriesStore = SeriesStore.initNewStoreForInMemTest();

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);

        long beforeMemory = MemoryUsageUtils.getUsedMemory();
        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
            indexWithGeoHashSemiSplit.updateIndex(point);
        }
        long afterMemory = MemoryUsageUtils.getUsedMemory();
        System.out.println("used memory: " + ((afterMemory - beforeMemory) / (1024 * 1024)) + " MB");
        System.out.println(indexWithGeoHashSemiSplit.printStatus());

        Random random = new Random(1);
        for (int i = 0; i < querySetSize; i++) {
            int index = random.nextInt(trajectoryPoints.size());
            TrajectoryPoint point = trajectoryPoints.get(index);

            double xLow = point.getLongitude();
            double xHigh = xLow + 1 * queryRegion;
            double yLow = point.getLatitude();
            double yHigh = yLow + 1 * queryRegion;
            queryPredicateList.add(new SpatialBoundingBox(new Point(xLow, yLow), new Point(xHigh, yHigh)));
        }

        long startTime = System.currentTimeMillis();
        for (SpatialBoundingBox spatialBoundingBox : queryPredicateList) {
            Set<String> result = indexWithGeoHashSemiSplit.searchForSpatial(spatialBoundingBox);
            //System.out.println(result.size());
            List<TrajectoryPoint> finalResult = seriesStore.refineReturnPoints(result, spatialBoundingBox);
            //System.out.println(finalResult.size());
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("consumed time: " + (stopTime - startTime) + " ms");
    }

    public static void testPhy() {
        long listSize = 10000000;

        int querySetSize = 500;

        double queryRegion = 0.001;

        int shiftLength = 16;

        int postingListCapacity = 100;

        List<SpatialBoundingBox> queryPredicateList;

        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit;

        SeriesStore seriesStore;

        queryPredicateList = new ArrayList<>();
        seriesStore = SeriesStore.initNewStoreForInMemTest();
        indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(shiftLength, postingListCapacity, seriesStore);

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);

        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
            indexWithGeoHashPhysicalSplit.updateIndex(point);
        }
        System.out.println(indexWithGeoHashPhysicalSplit.printStatus());

        Random random = new Random(1);
        for (int i = 0; i < querySetSize; i++) {
            int index = random.nextInt(trajectoryPoints.size());
            TrajectoryPoint point = trajectoryPoints.get(index);

            double xLow = point.getLongitude();
            double xHigh = xLow + 1 * queryRegion;
            double yLow = point.getLatitude();
            double yHigh = yLow + 1 * queryRegion;
            queryPredicateList.add(new SpatialBoundingBox(new Point(xLow, yLow), new Point(xHigh, yHigh)));
        }

        long startTime = System.currentTimeMillis();
        for (SpatialBoundingBox spatialBoundingBox : queryPredicateList) {
            Set<String> result = indexWithGeoHashPhysicalSplit.searchForSpatial(spatialBoundingBox);
            System.out.println(result.size());
            List<TrajectoryPoint> finalResult = seriesStore.refineReturnPoints(result, spatialBoundingBox);
            System.out.println(finalResult.size());
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("consumed time: " + (stopTime - startTime) + " ms");
    }

    public static void testRStar() {
        long listSize = 10000000;

        int querySetSize = 500;

        double queryRegion = 0.001;

        List<SpatialBoundingBox> queryPredicateList;

        HeadChunkIndexWithRStartreeMBR indexWithRStartreeMBR;

        SeriesStore seriesStore;

        queryPredicateList = new ArrayList<>();
        indexWithRStartreeMBR = new HeadChunkIndexWithRStartreeMBR();
        seriesStore = SeriesStore.initNewStoreForInMemTest();

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);

        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);
            seriesStore.appendSeriesPoint(point);
            indexWithRStartreeMBR.updateIndex(point);
        }
        System.out.println(indexWithRStartreeMBR.printStatus());

        Random random = new Random(1);
        for (int i = 0; i < querySetSize; i++) {
            int index = random.nextInt(trajectoryPoints.size());
            TrajectoryPoint point = trajectoryPoints.get(index);

            double xLow = point.getLongitude();
            double xHigh = xLow + 1 * queryRegion;
            double yLow = point.getLatitude();
            double yHigh = yLow + 1 * queryRegion;
            queryPredicateList.add(new SpatialBoundingBox(new Point(xLow, yLow), new Point(xHigh, yHigh)));
        }

        long startTime = System.currentTimeMillis();
        for (SpatialBoundingBox spatialBoundingBox : queryPredicateList) {
            Set<String> result = indexWithRStartreeMBR.searchForSpatial(spatialBoundingBox);
            System.out.println(result.size());
            List<TrajectoryPoint> finalResult = seriesStore.refineReturnPoints(result, spatialBoundingBox);
            System.out.println(finalResult.size());
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("consumed time: " + (stopTime - startTime) + " ms");
    }

}
