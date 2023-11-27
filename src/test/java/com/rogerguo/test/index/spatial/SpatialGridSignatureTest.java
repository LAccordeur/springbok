package com.rogerguo.test.index.spatial;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.*;

public class SpatialGridSignatureTest {

    @Test
    public void generateSignature() {

        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(0.1, 0.1));
        List<Point> pointList = new ArrayList<>();
        pointList.add(new Point(0, 0));
        pointList.add(new Point(0, 0.1));
        pointList.add(new Point(0.1, 0));
        pointList.add(new Point(0.1, 0.1));
        pointList.add(new Point(0.04, 0.04));

        SpatialGridSignature.generateSignature(spatialBoundingBox, pointList, 4 ,4);
    }

    @Test
    public void generateSignatureExtremeCase() {
        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(1, 1), new Point(1, 1));
        List<Point> pointList = new ArrayList<>();
        pointList.add(new Point(1, 1));

        byte[] signature = SpatialGridSignature.generateSignature(spatialBoundingBox, pointList, 4 ,4);

        SpatialBoundingBox predicate = new SpatialBoundingBox(new Point(1, 1), new Point(2, 2));
        boolean result = SpatialGridSignature.checkOverlap(predicate, spatialBoundingBox, signature, 4, 4);
        System.out.println(result);
    }

    @Test
    public void handleVirtualLineTest() {

        BitSet bitSet = new BitSet(16);
        bitSet.set(0);
        bitSet.set(8);
        //bitSet.set(3);
        System.out.println(bitSet.toString());
        BitSet bitsetHandled = SpatialGridSignature.handleVirtualLine(bitSet, 4, 4);
        System.out.println(bitsetHandled.toString());
    }

}