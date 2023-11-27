package com.rogerguo.test.store;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class HeadChunkIndexWithRtreeTest {

    private HeadChunkIndexWithRtree headChunkIndexWithRtree = new HeadChunkIndexWithRtree(8);

    @Test
    public void updateIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithRtree.updateIndex(point);
        }
        System.out.println(headChunkIndexWithRtree.printStatus());

    }

    @Test
    public void removeFromIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithRtree.updateIndex(point);
        }
        List<TrajectoryPoint> pointList1 = new ArrayList<>();
        pointList1.add(new TrajectoryPoint("T002", 3, 67, 13));
        Chunk chunk = new Chunk("T002");
        chunk.setChunk(pointList1);
        headChunkIndexWithRtree.removeFromIndex(chunk);
        System.out.println("finish");
    }

    @Test
    public void searchForSpatial() {

        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(67, 13), new Point(67, 13));

        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithRtree.updateIndex(point);
        }
        System.out.println("finish");
        System.out.println(headChunkIndexWithRtree.searchForSpatial(spatialBoundingBox));

        List<TrajectoryPoint> pointList1 = new ArrayList<>();
        pointList1.add(new TrajectoryPoint("T001", 3, 67, 13));
        Chunk chunk = new Chunk("T002");
        chunk.setChunk(pointList1);
        headChunkIndexWithRtree.removeFromIndex(chunk);
        System.out.println(headChunkIndexWithRtree.searchForSpatial(spatialBoundingBox));

    }

    private List<TrajectoryPoint> generateSyntheticPoint() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint point = new TrajectoryPoint("T001", 1, 22, 34);
        pointList.add(point);
        TrajectoryPoint point1 = new TrajectoryPoint("T001", 2, 23, 34);
        pointList.add(point1);
        TrajectoryPoint point2 = new TrajectoryPoint("T002", 3, 67, 13);
        pointList.add(point2);

        return pointList;
    }
}