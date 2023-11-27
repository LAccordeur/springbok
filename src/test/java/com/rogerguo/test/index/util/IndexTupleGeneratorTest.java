package com.rogerguo.test.index.util;

import com.rogerguo.test.common.SpatialTemporalBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import org.junit.Test;

import java.util.List;

public class IndexTupleGeneratorTest {

    @Test
    public void generateRandomDataForIndexTest() {
        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateRandomTupleForIndexTest(3, 4);
        System.out.println(metaList);
    }

    @Test
    public void generateSyntheticTupleForIndexTest() {
        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(4, 1, 10);
        System.out.println(metaList);
    }

    @Test
    public void generateRandomTrajectoryPoints() {

        List<TrajectoryPoint> result = IndexTupleGenerator.generateRandomTrajectoryPoints("test", 5);
        System.out.println(result);
    }

    @Test
    public void getBoundingBox() {

        List<TrajectoryPoint> result = IndexTupleGenerator.generateRandomTrajectoryPoints("test", 2);
        System.out.println(result);
        SpatialTemporalBoundingBox boundingBox = IndexTupleGenerator.getBoundingBox(result);
        System.out.println(boundingBox);
    }
}