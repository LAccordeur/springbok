package com.rogerguo.test.store;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class HeadChunkIndexWithGeoHashPhysicalSplitTest {

    SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
    private HeadChunkIndexWithGeoHashPhysicalSplit physicalSplitIndex = new HeadChunkIndexWithGeoHashPhysicalSplit(seriesStore);

    @Test
    public void updateIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
            physicalSplitIndex.updateIndex(point);
        }
        System.out.println(physicalSplitIndex);
        System.out.println("finish");

    }

    @Test
    public void updateIndexLarge() {
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(1000, 10, 10);

        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(32, 100, seriesStore);

        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
            indexWithGeoHashPhysicalSplit.updateIndex(point);
        }
    }

    @Test
    public void updateIndexSplit() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithGeoHashPhysicalSplit index = new HeadChunkIndexWithGeoHashPhysicalSplit(8, 2, seriesStore);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(200, 0.002, 0.001);

        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);

        }

        System.out.println(index.getDepthTable());
    }

    @Test
    public void removeFromIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
            physicalSplitIndex.updateIndex(point);
        }
        System.out.println(physicalSplitIndex);
        physicalSplitIndex.removeFromIndex(new Chunk("T001"));
        physicalSplitIndex.removeFromIndex(new Chunk("T002"));
        physicalSplitIndex.removeFromIndex(new Chunk("T003"));
        System.out.println("finish");
    }

    @Test
    public void removeFromIndexMerge() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithGeoHashPhysicalSplit index = new HeadChunkIndexWithGeoHashPhysicalSplit(8, 4, seriesStore);
        List<Chunk> chunkList = new ArrayList<>();
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(20, 0.002, 0.001);

        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
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
            seriesStore.appendSeriesPoint(point);
            physicalSplitIndex.updateIndex(point);
        }
        System.out.println(physicalSplitIndex.searchForSpatial(spatialBoundingBox));
        physicalSplitIndex.removeFromIndex(new Chunk("T001"));
        physicalSplitIndex.removeFromIndex(new Chunk("T002"));
        physicalSplitIndex.removeFromIndex(new Chunk("T004"));
        System.out.println(physicalSplitIndex.searchForSpatial(spatialBoundingBox));

        System.out.println("finish");
    }

    @Test
    public void searchLarge() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithGeoHashPhysicalSplit index = new HeadChunkIndexWithGeoHashPhysicalSplit(8, 4, seriesStore);
        List<Chunk> chunkList = new ArrayList<>();
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(40, 0.004, 0.004);

        for (TrajectoryPoint point : pointList) {
            seriesStore.appendSeriesPoint(point);
            index.updateIndex(point);
            Chunk chunk = new Chunk(point.getOid());
            chunkList.add(chunk);
        }

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(0.0001, 0.0005));
        Set<String> result = index.searchForSpatial(boundingBox);
        System.out.println(result.size());
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