package com.rogerguo.test.store;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.util.ZCurve;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class HeadChunkIndexWithGeoHashSemiSplitTest {

    private HeadChunkIndexWithGeoHashSemiSplit headChunkIndexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

/*
    @Test
    public void generateFourSubGrids() {
        List<InvertedIndexKey> result = headChunkIndexWithGeoHashSemiSplit.generateFourSubGridIds(256,(byte)1);
        System.out.println(0b000100000000);
        System.out.println(0b000101000000);
        System.out.println(0b000110000000);
        System.out.println(0b000111000000);


        System.out.println(result);
    }
*/

    @Test
    public void generateSubGrids() {
        ZCurve zCurve = new ZCurve();

        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

        List<InvertedIndexKey> result = index.generateSubGridIds(256, (byte) 2, 0, 16, 6, 22);

        // expected
        System.out.println(zCurve.getCurveValue(0, 16)); // depth 1
        System.out.println(zCurve.getCurveValue(0, 16)); // depth 2
        System.out.println(zCurve.getCurveValue(4, 16)); // depth 2
        System.out.println(zCurve.getCurveValue(0, 20)); // depth 2
        System.out.println(zCurve.getCurveValue(4, 20)); // depth 2

        System.out.println(result);
        System.out.println(result.size());
        for (InvertedIndexKey key : result) {
            System.out.println(Long.toBinaryString(key.getGridId()));
        }
    }

    @Test
    public void generateSubGridsOutOfDepth() {
        ZCurve zCurve = new ZCurve();

        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

        List<InvertedIndexKey> result = index.generateSubGridIds(256, (byte) 5, 0, 16, 6, 22);

        // expected
        /*System.out.println(zCurve.getCurveValue(0, 16)); // depth 1
        System.out.println(zCurve.getCurveValue(0, 16)); // depth 2
        System.out.println(zCurve.getCurveValue(4, 16)); // depth 2
        System.out.println(zCurve.getCurveValue(0, 20)); // depth 2
        System.out.println(zCurve.getCurveValue(4, 20)); // depth 2*/

        System.out.println(result);
        System.out.println(result.size());
        for (InvertedIndexKey key : result) {
            System.out.println(Long.toBinaryString(key.getGridId()));
        }
    }


    @Test
    public void generateFourSubGridKeys() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);
        List<InvertedIndexKey> result = index.generateForSubGridKeys(InvertedIndexKey.generateInvertedIndex(256, (byte)0));
        System.out.println(0b000100000000);
        System.out.println(0b000101000000);
        System.out.println(0b000110000000);
        System.out.println(0b000111000000);


        System.out.println(result);
    }

    @Test
    public void generateFourSubGridKeysOutOfDepth() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);
        List<InvertedIndexKey> result = index.generateForSubGridKeys(InvertedIndexKey.generateInvertedIndex(256, (byte)1));
        System.out.println(0b000100000000);
        System.out.println(0b000100010000);
        System.out.println(0b000100100000);
        System.out.println(0b000100110000);

        System.out.println(result);
        System.out.println();

        result = index.generateForSubGridKeys(InvertedIndexKey.generateInvertedIndex(256, (byte)2));
        System.out.println(0b000100000000);
        System.out.println(0b000100000100);
        System.out.println(0b000100001000);
        System.out.println(0b000100001100);

        System.out.println(result);
        System.out.println();

        result = index.generateForSubGridKeys(InvertedIndexKey.generateInvertedIndex(256, (byte)3));
        System.out.println(0b000100000000);
        System.out.println(0b000100000001);
        System.out.println(0b000100000010);
        System.out.println(0b000100000011);

        System.out.println(result);
        System.out.println();

        result = index.generateForSubGridKeys(InvertedIndexKey.generateInvertedIndex(256, (byte)4));
        System.out.println(0b000100000000);

        System.out.println(result);
        System.out.println();

    }

/*    @Test
    public void generateFourSubGridsOutOfDepth() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

        List<InvertedIndexKey> result = index.generateFourSubGridIds(256,(byte)4);
        System.out.println(0b000100000000);
        System.out.println(0b000100000001);
        System.out.println(0b000100000010);
        System.out.println(0b000100000011);


        System.out.println(result);
    }*/

    @Test
    public void generateSubGridIds() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

        List<InvertedIndexKey> result = index.generateSubGridIds(256, (byte) 2);
        // * then 0001 0000 0000, 0001 0100 0000, 0001 1000 0000, 0001 1100 0000
        //     *      0001 0000 0000, 0001 0001 0000, 0001 0010 0000, 0001 0011 0000
        //     *      0001 0100 0000, 0001 0101 0000, 0001 0110 0000, 0001 0111 0000
        //     *      0001 1000 0000, 0001 1001 0000, 0001 1010 0000, 0001 1011 0000
        //     *      0001 1100 0000, 0001 1101 0000, 0001 1110 0000, 0001 1111 0000
        System.out.println(result);
        System.out.println(result.size());
        for (InvertedIndexKey key : result) {
            System.out.println(Long.toBinaryString(key.getGridId()));
        }

    }

    @Test
    public void generateSubGridIdsOutOfDepth() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

        List<InvertedIndexKey> result = index.generateSubGridIds(256, (byte) 5);
        // * then 0001 0000 0000, 0001 0100 0000, 0001 1000 0000, 0001 1100 0000
        //     *      0001 0000 0000, 0001 0001 0000, 0001 0010 0000, 0001 0011 0000
        //     *      0001 0100 0000, 0001 0101 0000, 0001 0110 0000, 0001 0111 0000
        //     *      0001 1000 0000, 0001 1001 0000, 0001 1010 0000, 0001 1011 0000
        //     *      0001 1100 0000, 0001 1101 0000, 0001 1110 0000, 0001 1111 0000
        System.out.println(result);
        System.out.println(result.size());
        for (InvertedIndexKey key : result) {
            System.out.println(Long.toBinaryString(key.getGridId()));
        }

    }

    @Test
    public void updateIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithGeoHashSemiSplit.updateIndex(point);
        }
        System.out.println(headChunkIndexWithGeoHashSemiSplit);
        System.out.println("finish");

    }

    @Test
    public void updateIndexSplit() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 2);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(200, 0.002, 0.001);

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
        }

        System.out.println(index.getDepthTable());
    }

    @Test
    public void removeFromIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithGeoHashSemiSplit.updateIndex(point);
        }
        System.out.println(headChunkIndexWithGeoHashSemiSplit);
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T001"));
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T002"));
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T003"));
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T004"));
        System.out.println("finish");

    }

    @Test
    public void removeFromIndexMerge() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 4);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(20, 0.002, 0.001);
        List<Chunk> chunkList = new ArrayList<>();

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
            Chunk chunk = new Chunk(point.getOid());
            chunkList.add(chunk);
        }

        for (Chunk chunk : chunkList) {
            index.removeFromIndex(chunk);
        }

        System.out.println(index.getDepthTable());
    }

    @Test
    public void searchForSpatial() {

        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(67, 13), new Point(67, 13));


        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithGeoHashSemiSplit.updateIndex(point);
        }
        System.out.println(headChunkIndexWithGeoHashSemiSplit.searchForSpatial(spatialBoundingBox));
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T001"));
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T002"));
        headChunkIndexWithGeoHashSemiSplit.removeFromIndex(new Chunk("T004"));
        System.out.println(headChunkIndexWithGeoHashSemiSplit.searchForSpatial(spatialBoundingBox));

        System.out.println("finish");
    }

    @Test
    public void searchForSpatialLarge() {

        List<SpatialBoundingBox> queryPredicateList = new ArrayList<>();
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 100);

        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(100000, 10, 10);

        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint point = trajectoryPoints.get(i);

            index.updateIndex(point);
        }

        Random random = new Random(1);
        for (int i = 0; i < 1000; i++) {
            double xLow = random.nextDouble()*10;
            double xHigh = xLow + 10 * 0.1;
            double yLow = random.nextDouble()*10;
            double yHigh = yLow + 10 * 0.1;
            queryPredicateList.add(new SpatialBoundingBox(new Point(xLow, yLow), new Point(xHigh, yHigh)));
        }

        for (SpatialBoundingBox boundingBox : queryPredicateList) {
            System.out.println(boundingBox);
            Set<String> result = index.searchForSpatial(boundingBox);
            System.out.println(result.size());
        }

    }

    @Test
    public void searchLarge() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit(8, 4);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(40, 0.004, 0.004);
        List<Chunk> chunkList = new ArrayList<>();

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
            Chunk chunk = new Chunk(point.getOid());
            chunkList.add(chunk);
        }

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(0.0001, 0.0005));
        Set<String> result = index.searchForSpatial(boundingBox);
        System.out.println(result.size());

        // check
        int count = 0;
        for (TrajectoryPoint point : pointList) {
            if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                count++;
            }
        }
        System.out.println("real count: " + count);
        System.out.println(index.printStatus());
    }

    private List<TrajectoryPoint> generateSyntheticPoint() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint point = new TrajectoryPoint("T001", 1, 22, 34);
        pointList.add(point);
        TrajectoryPoint point1 = new TrajectoryPoint("T002", 1, 22, 34);
        pointList.add(point1);
        TrajectoryPoint point2 = new TrajectoryPoint("T003", 1, 22, 34);
        pointList.add(point2);
        TrajectoryPoint point3 = new TrajectoryPoint("T004", 1, 67, 13);
        pointList.add(point3);
        TrajectoryPoint point4 = new TrajectoryPoint("T005", 1, 22, 34);
        pointList.add(point4);

        return pointList;
    }
}