package com.rogerguo.test.store;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;

import java.util.HashSet;
import java.util.Set;

/**
 * @author yangguo
 * @create 2021-11-22 3:34 PM
 **/
@Deprecated
public class HeadChunkIndexWithRtree implements HeadChunkIndex {

    private int nodeSize;

    public HeadChunkIndexWithRtree(int nodeSize) {
        this.nodeSize = nodeSize;
        this.rtree = RTree.maxChildren(nodeSize).create();
    }

    public HeadChunkIndexWithRtree() {}

    private RTree<String, Geometry> rtree = RTree.create();

    @Override
    public String printStatus() {

        String status = "CONFIG: nodeSize = " + rtree.context().maxChildren() + "; Splitter = " + rtree.context().splitter().toString()+ "\n";
        status = status + "STATUS: depth of the tree: " + rtree.calculateDepth() + "; entry size: " + rtree.size() + "\n";
        return status;
    }

    @Override
    public void updateIndex(TrajectoryPoint point) {
        rtree = rtree.add(point.getOid(), Geometries.point(point.getLongitude(), point.getLatitude()));
    }

    @Override
    public void removeFromIndex(Chunk headChunk) {
        for (TrajectoryPoint point : headChunk.getChunk()) {
            rtree = rtree.delete(headChunk.getSid(), Geometries.point(point.getLongitude(), point.getLatitude()));
        }
    }

    @Override
    public Set<String> searchForSpatial(SpatialBoundingBox spatialBoundingBox) {
        Rectangle rectangle = Geometries.rectangle(spatialBoundingBox.getLowerLeft().getLongitude(), spatialBoundingBox.getLowerLeft().getLatitude(),
                spatialBoundingBox.getUpperRight().getLongitude(), spatialBoundingBox.getUpperRight().getLatitude());
        Iterable<Entry<String, Geometry>> result = rtree.search(rectangle).toBlocking().toIterable();

        Set<String> seriesIdSet = new HashSet<>();
        for (Entry<String, Geometry> item : result) {
            seriesIdSet.add(item.value());
        }

        return seriesIdSet;
    }


}
