package com.rogerguo.test.store;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;

import java.util.*;

/**
 * @author yangguo
 * @create 2021-10-04 4:11 PM
 **/
@Deprecated
public class HeadChunkIndexWithTwoLevelGrid implements HeadChunkIndex {

    private Map<Long, Set<String>> invertedIndexForHeadChunk = new HashMap<>();  //key is grid id which is for spatio-temporal query, value is a list of sid

    private Map<String, SpatialBoundingBox> spatialBoundingBoxForHeadChunk = new HashMap<>();  // key is sid, value is the spatial bounding box for head chunk

    private TwoLevelGridIndex twoLevelGridIndex = new TwoLevelGridIndex();

    @Override
    public String printStatus() {
        return null;
    }

    public void updateIndex(TrajectoryPoint point) {

        if (spatialBoundingBoxForHeadChunk.containsKey(point.getOid())) {
            SpatialBoundingBox headChunkBoundingBox = spatialBoundingBoxForHeadChunk.get(point.getOid());
            Point spatialPoint = new Point(point.getLongitude(), point.getLatitude());
            // check if new point change the bounding box of the head chunk
            if (!twoLevelGridIndex.checkBoundingBoxContainPoint(headChunkBoundingBox, spatialPoint)) {
                // if not contained, we need to do updates
                // 1. update bounding box
                SpatialBoundingBox newBoundingBox = twoLevelGridIndex.extendBoundingBoxByPoint(headChunkBoundingBox, spatialPoint);
                spatialBoundingBoxForHeadChunk.put(point.getOid(), newBoundingBox);
                // 2. update inverted index
                List<Long> newGridIdList = twoLevelGridIndex.calculateNonOverlappedGrids(headChunkBoundingBox, newBoundingBox);
                for (Long gridId : newGridIdList) {
                    if (invertedIndexForHeadChunk.containsKey(gridId)) {
                        Set<String> sidSet = invertedIndexForHeadChunk.get(gridId);
                        sidSet.add(point.getOid());
                    } else {
                        Set<String> sidSet = new HashSet<>();
                        sidSet.add(point.getOid());
                        invertedIndexForHeadChunk.put(gridId, sidSet);
                    }
                }
            }

        } else {
            // this is the first point in the head chunk
            SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(point.getLongitude(), point.getLatitude()), new Point(point.getLongitude(), point.getLatitude()));
            spatialBoundingBoxForHeadChunk.put(point.getOid(), boundingBox);

            List<Long> indexGridList = twoLevelGridIndex.toIndexGrids(boundingBox);
            for (Long gridId : indexGridList) {
                if (invertedIndexForHeadChunk.containsKey(gridId)) {
                    Set<String> sidSet = invertedIndexForHeadChunk.get(gridId);
                    sidSet.add(point.getOid());
                } else {
                    Set<String> sidSet = new HashSet<>();
                    sidSet.add(point.getOid());
                    invertedIndexForHeadChunk.put(gridId, sidSet);
                }
            }
        }
    }

    public void removeFromIndex(Chunk headChunk) {
        // remove from inverted index map
        List<Long> gridList = twoLevelGridIndex.toIndexGrids(spatialBoundingBoxForHeadChunk.get(headChunk.getSid()));
        for (Long gridId : gridList) {
            if (invertedIndexForHeadChunk.containsKey(gridId)) {
                invertedIndexForHeadChunk.get(gridId).remove(headChunk.getSid());
            }
        }

        // remove from bounding box map
        spatialBoundingBoxForHeadChunk.remove(headChunk.getSid());

    }

    public Set<String> searchForSpatial(SpatialBoundingBox boundingBox) {
        Set<String> sidSet = new HashSet<>();
        List<Long> gridList = twoLevelGridIndex.toIndexGrids(boundingBox);
        for (long grid : gridList) {
            sidSet.addAll(invertedIndexForHeadChunk.get(grid));
        }

        return sidSet;
    }

}