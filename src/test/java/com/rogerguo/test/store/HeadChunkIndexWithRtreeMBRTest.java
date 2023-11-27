package com.rogerguo.test.store;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class HeadChunkIndexWithRtreeMBRTest {

    private HeadChunkIndexWithRtreeMBR headChunkIndexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR(8);

    @Test
    public void updateIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithRtreeMBR.updateIndex(point);
        }
        headChunkIndexWithRtreeMBR.updateIndex(new TrajectoryPoint("T003", 2, 26, 39));
        System.out.println(headChunkIndexWithRtreeMBR);
        System.out.println(headChunkIndexWithRtreeMBR.printStatus());

    }

    @Test
    public void removeFromIndex() {
        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithRtreeMBR.updateIndex(point);
        }
        System.out.println(headChunkIndexWithRtreeMBR);
        headChunkIndexWithRtreeMBR.removeFromIndex(new Chunk("T001"));
        headChunkIndexWithRtreeMBR.removeFromIndex(new Chunk("T002"));
        headChunkIndexWithRtreeMBR.removeFromIndex(new Chunk("T003"));
        System.out.println(headChunkIndexWithRtreeMBR.printStatus());
    }

    @Test
    public void searchForSpatial() {
        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(67, 13), new Point(67, 13));


        List<TrajectoryPoint> pointList = generateSyntheticPoint();
        for (TrajectoryPoint point : pointList) {
            headChunkIndexWithRtreeMBR.updateIndex(point);
        }
        System.out.println(headChunkIndexWithRtreeMBR.searchForSpatial(spatialBoundingBox));
        headChunkIndexWithRtreeMBR.removeFromIndex(new Chunk("T001"));
        headChunkIndexWithRtreeMBR.removeFromIndex(new Chunk("T002"));
        headChunkIndexWithRtreeMBR.removeFromIndex(new Chunk("T004"));
        System.out.println(headChunkIndexWithRtreeMBR.searchForSpatial(spatialBoundingBox));

        System.out.println(headChunkIndexWithRtreeMBR.printStatus());
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