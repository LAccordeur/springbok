package com.rogerguo.test.index.util;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialIndexNode;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.spatial.SpatialGridSignature;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class IndexSerializationUtilTest {

    @Test
    public void serializeLeafSpatialNode() {

        SpatialTemporalTree tree = new SpatialTemporalTree(SpatialTemporalTree.getDefaultIndexConfiguration());
        SpatialIndexNode spatialIndexNode = new SpatialIndexNode(null, tree);
        List<TrajectoryPoint> pointList = new ArrayList<>();
        pointList.add(new TrajectoryPoint("test", 0, 0, 0));
        pointList.add(new TrajectoryPoint("test", 12, 1, 1));
        spatialIndexNode.insert(new TrajectorySegmentMeta(0, 12, new Point(0, 0), new Point(1, 2), "test", "1", pointList));
        String result = IndexSerializationUtil.serializeLeafSpatialNode(spatialIndexNode);
        System.out.println(result);
        SpatialIndexNode reback = IndexSerializationUtil.deserializeLeafSpatialNode(result);
        System.out.println(reback);
    }

}