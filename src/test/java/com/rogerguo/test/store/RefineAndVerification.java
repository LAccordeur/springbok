package com.rogerguo.test.store;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author yangguo
 * @create 2021-12-24 9:56 AM
 **/
public class RefineAndVerification {

    private SpatialBoundingBox testBoundingBox1 = new SpatialBoundingBox(new Point(0, 0), new Point(0.05, 0.05));

    private SpatialBoundingBox testBoundingBox2 = new SpatialBoundingBox(new Point(0.01, 0.02), new Point(0.03, 0.035));

    private List<TrajectoryPoint> pointList1000 = SyntheticDataGenerator.generateRandomDistributedDataset(1000, 0.1, 0.1);

    private List<TrajectoryPoint> pointList10000 = SyntheticDataGenerator.generateRandomDistributedDataset(10000, 0.1, 0.1);

    @Test
    public void refineAndVerifyGeoHashIndex() {
        HeadChunkIndexWithGeoHash indexWithGeoHash = new HeadChunkIndexWithGeoHash();
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        for (TrajectoryPoint point : pointList1000) {
            seriesStore.appendSeriesPoint(point);
            indexWithGeoHash.updateIndex(point);
        }
        Set<String> resultIdSet = indexWithGeoHash.searchForSpatial(testBoundingBox1);
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox1);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox1, pointList1000, refinedPointList);
        System.out.println(indexWithGeoHash.printStatus());

    }

    @Test
    public void refineAndVerifyGeoHashSemi() {
        HeadChunkIndexWithGeoHashSemiSplit index = new HeadChunkIndexWithGeoHashSemiSplit();
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        for (TrajectoryPoint point : pointList10000) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
        }
        Set<String> resultIdSet = index.searchForSpatial(testBoundingBox2);
        System.out.println("result without refinement: " + resultIdSet.size());
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox2);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox2, pointList10000, refinedPointList);
        System.out.println(index.printStatus());
    }

    @Test
    public void refineAndVerifyGeoHashPhy() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithGeoHashPhysicalSplit index = new HeadChunkIndexWithGeoHashPhysicalSplit(seriesStore);

        for (TrajectoryPoint point : pointList1000) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
        }
        Set<String> resultIdSet = index.searchForSpatial(testBoundingBox2);
        System.out.println("result without refinement: " + resultIdSet.size());
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox2);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox2, pointList1000, refinedPointList);
        System.out.println(index.printStatus());
    }

    @Test
    public void refineAndVerifyRStarTree() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithRStartree index = new HeadChunkIndexWithRStartree();

        for (TrajectoryPoint point : pointList10000) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
        }
        TrajectoryPoint testPoint = new TrajectoryPoint("1", 1, 1 , 1);
        seriesStore.appendSeriesPoint(testPoint);
        index.updateIndex(testPoint);

        Set<String> resultIdSet = index.searchForSpatial(testBoundingBox1);

        int countWithoutRefine = 0;
        for (String id : resultIdSet) {
            countWithoutRefine = countWithoutRefine + seriesStore.getSeriesStore().get(id).getHeadChunk().size();
        }

        System.out.println("count without refinement: " + countWithoutRefine);
        System.out.println("result without refinement: " + resultIdSet.size());
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox1);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox1, pointList10000, refinedPointList);
        System.out.println(index.printStatus());
    }

    @Test
    public void refineAndVerifyRStarMBR() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithRStartreeMBR index = new HeadChunkIndexWithRStartreeMBR();

        for (TrajectoryPoint point : pointList10000) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
        }
        TrajectoryPoint testPoint = new TrajectoryPoint("1", 1, 1 , 1);
        seriesStore.appendSeriesPoint(testPoint);
        index.updateIndex(testPoint);

        Set<String> resultIdSet = index.searchForSpatial(testBoundingBox1);

        int countWithoutRefine = 0;
        for (String id : resultIdSet) {
            countWithoutRefine = countWithoutRefine + seriesStore.getSeriesStore().get(id).getHeadChunk().size();
        }

        System.out.println("count without refinement: " + countWithoutRefine);
        System.out.println("result without refinement: " + resultIdSet.size());
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox1);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox1, pointList10000, refinedPointList);
        System.out.println(index.printStatus());
    }

    @Test
    public void refineAndVerifyRtree() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithRtree index = new HeadChunkIndexWithRtree();

        for (TrajectoryPoint point : pointList1000) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
        }
        TrajectoryPoint testPoint = new TrajectoryPoint("1", 1, 1 , 1);
        seriesStore.appendSeriesPoint(testPoint);
        index.updateIndex(testPoint);

        Set<String> resultIdSet = index.searchForSpatial(testBoundingBox2);

        int countWithoutRefine = 0;
        for (String id : resultIdSet) {
            countWithoutRefine = countWithoutRefine + seriesStore.getSeriesStore().get(id).getHeadChunk().size();
        }

        System.out.println("count without refinement: " + countWithoutRefine);
        System.out.println("result without refinement: " + resultIdSet.size());
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox2);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox2, pointList1000, refinedPointList);
        System.out.println(index.printStatus());
    }

    @Test
    public void refineAndVerifyRTreeMBR() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithRtreeMBR index = new HeadChunkIndexWithRtreeMBR();

        for (TrajectoryPoint point : pointList1000) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
        }
        TrajectoryPoint testPoint = new TrajectoryPoint("1", 1, 1 , 1);
        seriesStore.appendSeriesPoint(testPoint);
        index.updateIndex(testPoint);

        Set<String> resultIdSet = index.searchForSpatial(testBoundingBox2);

        int countWithoutRefine = 0;
        for (String id : resultIdSet) {
            countWithoutRefine = countWithoutRefine + seriesStore.getSeriesStore().get(id).getHeadChunk().size();
        }

        System.out.println("count without refinement: " + countWithoutRefine);
        System.out.println("result without refinement: " + resultIdSet.size());
        List<TrajectoryPoint> refinedPointList = seriesStore.refineReturnPoints(resultIdSet, testBoundingBox2);
        System.out.println("query returned count: " + refinedPointList.size());
        verify(testBoundingBox2, pointList1000, refinedPointList);
        System.out.println(index.printStatus());
    }

    public void verify(SpatialBoundingBox boundingBox, List<TrajectoryPoint> dataList, List<TrajectoryPoint> queryResultList) {
        List<TrajectoryPoint> result = new ArrayList<>();
        int matchCount = 0;

        for (TrajectoryPoint point : dataList) {
            if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                result.add(point);
                if (queryResultList.contains(point)) {
                    matchCount = matchCount + 1;
                }
            }
        }

        System.out.println("actual count: " + result.size());
        System.out.println("match count: " + matchCount);
    }

}
