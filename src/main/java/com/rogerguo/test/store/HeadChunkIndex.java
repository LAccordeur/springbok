package com.rogerguo.test.store;

import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;

import java.util.Set;

public interface HeadChunkIndex {

    void updateIndex(TrajectoryPoint point);

    void removeFromIndex(Chunk headChunk);

    Set<String> searchForSpatial(SpatialBoundingBox spatialBoundingBox);

    String printStatus();

}
