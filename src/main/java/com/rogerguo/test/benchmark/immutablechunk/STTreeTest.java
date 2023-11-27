package com.rogerguo.test.benchmark.immutablechunk;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.NodeTuple;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.index.util.TreePrinter;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2021-12-28 3:07 PM
 **/
public class STTreeTest {

    public static void main(String[] args) {
        testSpatioTemporalQuery();
    }

    public static void testGridIdParsingTime() {
        TwoLevelGridIndex twoLevelGridIndex = new TwoLevelGridIndex();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 195100; i++) {
            twoLevelGridIndex.getBoundingBoxByGridId(i);
        }
        long stop = System.currentTimeMillis();
        System.out.println("time: " + (stop - start) + " ms");
    }

    public static void testSpatioTemporalQuery() {
        int listSize = 100000;
        boolean preciseSpatialIndex = false;
        List<SpatialTemporalRangeQueryPredicate> queryPredicateList = new ArrayList<>();
        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        IndexConfiguration indexConfiguration = new IndexConfiguration(256, true, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, true);
        SpatialTemporalTree indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

        List<TrajectorySegmentMeta> metaList = SyntheticDataGenerator.generateGaussianDistributedIndexEntries(listSize, 1, 1);
        for (TrajectorySegmentMeta segmentMeta : metaList) {
            indexTree.insert(segmentMeta);
        }

        int timeRange = 30;
        double spatialRange = 0.02;
        Random random = new Random(3);
        for (int i = 0; i < 500; i++) {
            int randomIndex = Math.abs(random.nextInt(listSize));
            TrajectorySegmentMeta meta = metaList.get(randomIndex);
            TrajectoryPoint refPoint = meta.getTrajectoryPointList().get(0);
            long startTime = refPoint.getTimestamp();
            long stopTime = startTime + timeRange;
            Point lowerLeft = new Point(refPoint.getLongitude(), refPoint.getLatitude());
            Point upperRight = new Point(refPoint.getLongitude()+spatialRange, refPoint.getLatitude()+spatialRange);
            SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(startTime, stopTime, lowerLeft, upperRight);
            queryPredicateList.add(predicate);
        }

        List<TrajectoryPoint> allPoints = new ArrayList<>();
        for (TrajectorySegmentMeta meta : metaList) {
            allPoints.addAll(meta.getTrajectoryPointList());
        }
        int totalUnRefinedSize = 0;
        long start = System.currentTimeMillis();
        for (SpatialTemporalRangeQueryPredicate predicate : queryPredicateList) {
            List<NodeTuple> tuples = indexTree.searchForSpatialTemporal(predicate);
            totalUnRefinedSize = totalUnRefinedSize + tuples.size();
            //System.out.println(tuples.size());
            List<TrajectoryPoint> pointList = RefineUtilForTest.refineSpatioTemporal(predicate, tuples, metaList);
            verifySpatioTemporal(predicate, allPoints, pointList);
        }
        long stop = System.currentTimeMillis();
        System.out.println(indexTree.printStatus());
        System.out.println("take " + (stop - start) + " ms");
        System.out.println("num of tuples: " + totalUnRefinedSize);
    }

    private static void verifySpatioTemporal(SpatialTemporalRangeQueryPredicate predicate, List<TrajectoryPoint> rawPointList, List<TrajectoryPoint> result) {
        int matchedCount = 0;
        int actualCount = 0;

        System.out.println(predicate);

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        for (TrajectoryPoint point : rawPointList) {
            if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point) && predicate.getStartTimestamp() <= point.getTimestamp()
                    && predicate.getStopTimestamp() >= point.getTimestamp()) {
                actualCount = actualCount + 1;

                if (result.contains(point)) {
                    matchedCount++;
                } else {
                    System.out.println(point);
                }
            }
        }

        System.out.println("actual count: " + actualCount);
        System.out.println("matched count: " + matchedCount);
        System.out.println();
    }

    public static void testIdTemporalQuery() {
        int listSize = 100000;
        int nodeSize = 256;
        List<IdTemporalQueryPredicate> queryPredicateList = new ArrayList<>();
        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, true, bucketName, rootDirnameInBucket, region, true, true);
        SpatialTemporalTree indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

        List<TrajectorySegmentMeta> metaList = SyntheticDataGenerator.generateRandomDistributedIndexEntries(listSize, 1, 1);
        for (TrajectorySegmentMeta segmentMeta : metaList) {
            indexTree.insert(segmentMeta);
        }

        Random random = new Random(3);
        for (int i = 0; i < 500; i++) {
            int randomValue = Math.abs(random.nextInt(listSize));
            IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(randomValue, randomValue + 20, String.valueOf(randomValue));
            queryPredicateList.add(predicate);
        }

        long start = System.currentTimeMillis();
        for (IdTemporalQueryPredicate predicate : queryPredicateList) {
            List<NodeTuple> tuples = indexTree.searchForIdTemporal(predicate);
            List<TrajectoryPoint> result = RefineUtilForTest.refineIdTemporal(predicate, tuples, metaList);
            verifyIdTemporal(predicate, metaList.get(Integer.parseInt(predicate.getDeviceId())).getTrajectoryPointList(), result);
            //System.out.println(result.size());
        }
        long stop = System.currentTimeMillis();
        System.out.println("take " + (stop - start) + " ms");
    }

    private static void verifyIdTemporal(IdTemporalQueryPredicate predicate, List<TrajectoryPoint> pointList, List<TrajectoryPoint> result) {
        int matchedCount = 0;
        int actualCount = 0;
        for (TrajectoryPoint point : pointList) {
            if (predicate.getDeviceId().equals(point.getOid()) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                actualCount = actualCount + 1;
                if (result.contains(point)) {
                    matchedCount++;
                }
            }
        }

        System.out.println("actual count: " + actualCount);
        System.out.println("matched count: " + matchedCount);
        System.out.println();
    }

    public static void testInsertion() {
        int listSize = 1000000;
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1, 1);
        List<TrajectorySegmentMeta> segmentMetaList = new ArrayList<>();
        for (TrajectoryPoint point : pointList) {
            Point lowerLeft = new Point(point.getLongitude(), point.getLatitude());
            Point upperRight = new Point(point.getLongitude(), point.getLatitude());
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(point.getTimestamp(), point.getTimestamp(), lowerLeft, upperRight, point.getOid(), "test");
            List<TrajectoryPoint> pointList1 = new ArrayList<>();
            pointList1.add(point);
            meta.setTrajectoryPointList(pointList1);
            segmentMetaList.add(meta);
        }

        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        IndexConfiguration indexConfiguration = new IndexConfiguration(128, true, bucketName, rootDirnameInBucket, region, true, true);
        SpatialTemporalTree indexTree = new SpatialTemporalTree(indexConfiguration);

        long start = System.currentTimeMillis();
        for (int i = 0; i < segmentMetaList.size(); i++) {
            indexTree.insert(segmentMetaList.get(i));
            //System.out.println(i);
        }
        long stop = System.currentTimeMillis();
        System.out.println(indexTree.printStatus());
        //new TreePrinter(indexTree).print(System.out);
        System.out.println("finish");
        System.out.println("takes " + (stop - start) + " ms");
    }

}
