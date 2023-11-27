package com.rogerguo.test.store;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class HeadChunkIndexWithGeoHashTest {

    private HeadChunkIndexWithGeoHash headChunkIndexWithGeoHash = new HeadChunkIndexWithGeoHash();

    @Test
    public void updateIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithGeoHash.updateIndex(point);
        }
        System.out.println("finish");
    }

    @Test
    public void removeFromIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithGeoHash.updateIndex(point);
        }
        System.out.println(headChunkIndexWithGeoHash);
        Chunk chunk = new Chunk("T001");
        headChunkIndexWithGeoHash.removeFromIndex(chunk);
        System.out.println(headChunkIndexWithGeoHash);
        System.out.println("finish");

    }

    @Test
    public void searchForSpatial() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithGeoHash.updateIndex(point);
        }
        System.out.println(headChunkIndexWithGeoHash);

        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(22, 34), new Point(22.5, 34.3));
        Set<String> result = headChunkIndexWithGeoHash.searchForSpatial(spatialBoundingBox);
        System.out.println(result);
    }

    private List<TrajectoryPoint> generateSyntheticPoint() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint point = new TrajectoryPoint("T001", 1, 22, 34);
        pointList.add(point);
        TrajectoryPoint point1 = new TrajectoryPoint("T001", 2, 23, 34);
        pointList.add(point1);
        TrajectoryPoint point2 = new TrajectoryPoint("T002", 3, 67, 13);
        pointList.add(point2);
        TrajectoryPoint point3 = new TrajectoryPoint("T003", 3, 22.01, 34);
        pointList.add(point3);

        return pointList;
    }
}