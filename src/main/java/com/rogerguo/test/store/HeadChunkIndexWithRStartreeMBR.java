package com.rogerguo.test.store;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yangguo
 * @create 2021-12-07 1:02 PM
 **/
public class HeadChunkIndexWithRStartreeMBR implements HeadChunkIndex{

    private int nodeSize;

    public HeadChunkIndexWithRStartreeMBR(int nodeSize) {
        this.nodeSize = nodeSize;
        this.rtree = RTree.star().maxChildren(nodeSize).create();
    }

    public HeadChunkIndexWithRStartreeMBR() {}

    private RTree<String, Geometry> rtree = RTree.star().create();

    private Map<String, SpatialBoundingBox> spatialBoundingBoxForHeadChunk = new HashMap<>();  // key is sid, value is the spatial bounding box for head chunk

    @Override
    public String printStatus() {

        String status = "[RStar MBR]CONFIG: nodeSize = " + rtree.context().maxChildren() + "; Splitter = " + rtree.context().splitter().toString()+ "\n";
        status = status + "STATUS: depth of the tree: " + rtree.calculateDepth() + "; entry size: " + rtree.size() + "\n";
        return status;
    }

    @Override
    public void updateIndex(TrajectoryPoint point) {
        if (!spatialBoundingBoxForHeadChunk.containsKey(point.getOid())) {
            spatialBoundingBoxForHeadChunk.put(point.getOid(), new SpatialBoundingBox(point, point));
            rtree = rtree.add(point.getOid(), Geometries.rectangle(point.getLongitude(), point.getLatitude(), point.getLongitude(), point.getLatitude()));
        } else {
            SpatialBoundingBox spatialBoundingBox = extendBoundingBoxByPoint(spatialBoundingBoxForHeadChunk.get(point.getOid()), point);
            if (!(spatialBoundingBox == spatialBoundingBoxForHeadChunk.get(point.getOid()))) {
                SpatialBoundingBox oldSpatialBoundingBox = spatialBoundingBoxForHeadChunk.get(point.getOid());
                rtree = rtree.delete(point.getOid(), Geometries.rectangle(oldSpatialBoundingBox.getLowerLeft().getLongitude(), oldSpatialBoundingBox.getLowerLeft().getLatitude(),
                        oldSpatialBoundingBox.getUpperRight().getLongitude(), oldSpatialBoundingBox.getUpperRight().getLatitude()));
                rtree = rtree.add(point.getOid(), Geometries.rectangle(spatialBoundingBox.getLowerLeft().getLongitude(), spatialBoundingBox.getLowerLeft().getLatitude(),
                                                                            spatialBoundingBox.getUpperRight().getLongitude(), spatialBoundingBox.getUpperRight().getLatitude()));
                spatialBoundingBoxForHeadChunk.put(point.getOid(), spatialBoundingBox);
            }
        }

    }

    @Override
    public void removeFromIndex(Chunk headChunk) {
        SpatialBoundingBox boundingBox = spatialBoundingBoxForHeadChunk.get(headChunk.getSid());
        rtree = rtree.delete(headChunk.getSid(), Geometries.rectangle(boundingBox.getLowerLeft().getLongitude(), boundingBox.getLowerLeft().getLatitude(),
                                                                    boundingBox.getUpperRight().getLongitude(), boundingBox.getUpperRight().getLatitude()));
        spatialBoundingBoxForHeadChunk.remove(headChunk.getSid());
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


    public SpatialBoundingBox extendBoundingBoxByPoint(SpatialBoundingBox boundingBox, Point point) {
        String newXMarker;
        String newYMarker;

        if (point.getLongitude() < boundingBox.getLowerLeft().getLongitude()) {
            newXMarker = "less";
        } else if (point.getLongitude() > boundingBox.getUpperRight().getLongitude()) {
            newXMarker = "more";
        } else {
            newXMarker = "middle";
        }

        if (point.getLatitude() < boundingBox.getLowerLeft().getLatitude()) {
            newYMarker = "less";
        } else if (point.getLatitude() > boundingBox.getUpperRight().getLatitude()) {
            newYMarker = "more";
        } else {
            newYMarker = "middle";
        }

        if (newXMarker.equals("less") && newYMarker.equals("less")) {
            return new SpatialBoundingBox(point, boundingBox.getUpperRight());
        } else if (newXMarker.equals("less") && newYMarker.equals("middle")) {
            return new SpatialBoundingBox(new Point(point.getLongitude(), boundingBox.getLowerLeft().getLatitude()), boundingBox.getUpperRight());
        } else if (newXMarker.equals("less") && newYMarker.equals("more")) {
            return new SpatialBoundingBox(new Point(point.getLongitude(), boundingBox.getLowerLeft().getLatitude()), new Point(boundingBox.getUpperRight().getLongitude(), point.getLatitude()));
        } else if (newXMarker.equals("middle") && newYMarker.equals("less")) {
            return new SpatialBoundingBox(new Point(boundingBox.getLowerLeft().getLongitude(), point.getLatitude()), boundingBox.getUpperRight());
        } else if (newXMarker.equals("middle") && newYMarker.equals("middle")) {
            return boundingBox;
        } else if (newXMarker.equals("middle") && newYMarker.equals("more")) {
            return new SpatialBoundingBox(boundingBox.getLowerLeft(), new Point(boundingBox.getUpperRight().getLongitude(), point.getLatitude()));
        } else if (newXMarker.equals("more") && newYMarker.equals("less")) {
            return new SpatialBoundingBox(new Point(boundingBox.getLowerLeft().getLongitude(), point.getLatitude()), new Point(point.getLongitude(), boundingBox.getUpperRight().getLatitude()));
        } else if (newXMarker.equals("more") && newYMarker.equals("middle")) {
            return new SpatialBoundingBox(boundingBox.getLowerLeft(), new Point(point.getLongitude(), boundingBox.getUpperRight().getLatitude()));
        } else {
            // more && more
            return new SpatialBoundingBox(boundingBox.getLowerLeft(), point);
        }

    }
}
