package com.rogerguo.test.index.spatial;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.SpatialRange;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.util.IndexTupleGenerator;
import com.rogerguo.test.util.TrajectorySimulator;
import com.rogerguo.test.util.ZCurve;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TwoLevelGridIndexTest {

    TwoLevelGridIndex index = new TwoLevelGridIndex();

    @Test
    public void toCombinedGridIdentifier() {

        //HashMap hashMap = new HashMap();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 5; j++) {
                long id = index.toCombinedGridIdentifier(i, j);
                System.out.println("i: " + Long.toBinaryString(i));
                System.out.println("j: " + Long.toBinaryString(j));
                System.out.println("id: " + Long.toBinaryString(id));
                System.out.println(id);
                System.out.println();
            }
        }
    }

    @Test
    public void getOverLappedRange() {

        SpatialRange range1 = new SpatialRange(34, 35.5);
        SpatialRange range2 = new SpatialRange(37,35.5);
        SpatialRange result = index.getOverlappedRange(range1, range2);
        System.out.println(result);
    }

    @Test
    public void getOverlappedBoundBox() {
        SpatialBoundingBox boundingBox1 = new SpatialBoundingBox(new Point(3,4), new Point(5,8));
        SpatialBoundingBox boundingBox2 = new SpatialBoundingBox(new Point(1, 10), new Point(3, 11));
        SpatialBoundingBox result = index.getOverlappedBoundingBox(boundingBox1, boundingBox2);

        System.out.println(result);
    }

    @Test
    public void getBoundingBoxByLevelOneGridCoordinate() {
        SpatialBoundingBox result = index.getBoundingBoxByLevelOneGridCoordinate(1, 1);
        System.out.println(result);
    }

    @Test
    public void toIndexGrids() {
        SpatialBoundingBox box = new SpatialBoundingBox(new Point(0.00,0.00), new Point(0.5,0.5));
        List<Long> result= index.toIndexGrids(box);
        System.out.println(result);

    }

    @Test
    public void toPreciseGirds() {
        SpatialBoundingBox box = new SpatialBoundingBox(new Point(0.00,0.00), new Point(0.5,0.5));
        List<Point> list = new ArrayList<>();
        list.add(new Point(0, 0));
        list.add(new Point(0.5, 0.5));
        List<Long> result= index.toPreciseIndexGrids(box, list);
        System.out.println(result);
    }

    @Test
    public void toPreciseGridsBatch() {

        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(10000, 8, 10);
        long start = System.currentTimeMillis();
        for (TrajectorySegmentMeta meta : metaList) {
            index.toIndexGrids(new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight()));
        }
        long stop = System.currentTimeMillis();
        System.out.println("time: " + (stop - start));

    }

    @Test
    public void toPreciseGridsOptimized() {
        List<TrajectoryPoint> pointList = TrajectorySimulator.generateSyntheticTrajectory(1, 6);
        System.out.println(pointList);
        SpatialBoundingBox boundingBox = TrajectorySimulator.generateSpatialBoundingBox(pointList);

        long start = System.currentTimeMillis();
        Set<Long> result = index.toPreciseIndexGridsOptimized(pointList);
        long stop = System.currentTimeMillis();
        System.out.println("Optimized version takes " + (stop - start));
        System.out.println(result);

        /*result = index.toPreciseIndexGrids(boundingBox, pointList);
        long stop1 = System.currentTimeMillis();
        System.out.println("Old version takes " + (stop1 - stop));
        System.out.println(result);*/



    }

    @Test
    public void splitGridId() {

        long testId = index.toCombinedGridIdentifier(12, 45);
        System.out.println(testId);
        Map<String, Long> result = index.splitGridId(192);
        System.out.println(result);
        ZCurve zCurve = new ZCurve();
        System.out.println(zCurve.from2DCurveValue(192));

    }
}