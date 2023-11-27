package com.rogerguo.test.index;

import com.rogerguo.test.index.predicate.BasicQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.index.spatial.SpatialGridSignature;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @Description
 * @Date 2021/3/15 14:51
 * @Created by Yang GUO
 */
public class SpatialIndexNode extends TreeNode {

    List<SpatialIndexNodeTuple> tuples;

    private static Logger logger = LoggerFactory.getLogger(SpatialIndexNode.class);

    private static int xBitNum = StoreConfig.SPATIAL_BITMAP_X_BIT_NUM;

    private static int yBitNum = StoreConfig.SPATIAL_BITMAP_Y_BIT_NUM;

    public SpatialIndexNode(TreeNode parentNode, SpatialTemporalTree indexTree) {
        super(parentNode, indexTree);
        this.tuples = new ArrayList<SpatialIndexNodeTuple>();
    }

    public SpatialIndexNode() {}

    public boolean insert(NodeTuple tuple) {
        return true;
    }

    /**
     * search tuples according to spatial information
     * @param meta
     * @return
     */
    public List<NodeTuple> search(BasicQueryPredicate meta) {

        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();

        SpatialTemporalRangeQueryPredicate spatialTemporalPredicate = null;
        if (meta instanceof SpatialTemporalRangeQueryPredicate) {
            spatialTemporalPredicate = (SpatialTemporalRangeQueryPredicate) meta;
        } else {
            return resultTuples;
        }

        SpatialBoundingBox predicateBox = new SpatialBoundingBox(spatialTemporalPredicate.getLowerLeft(), spatialTemporalPredicate.getUpperRight());

        if (getIndexTree().getIndexConfiguration().isUsePreciseSpatialIndex()) {
            /*for (SpatialIndexNodeTuple indexNodeTuple : tuples) {
                if (isIntersected(indexNodeTuple, spatialTemporalPredicate)) {
                    resultTuples.add(indexNodeTuple);
                }
            }*/
            for (SpatialIndexNodeTuple mbrTuple : tuples) {
                //SpatialIndexNodeMBRTuple mbrTuple = (SpatialIndexNodeMBRTuple) indexNodeTuple;
                if (SpatialBoundingBox.getOverlappedBoundingBox(mbrTuple.getBoundingBox(), predicateBox) != null
                        && SpatialGridSignature.checkOverlap(predicateBox, mbrTuple.getBoundingBox(), mbrTuple.getSignature(), xBitNum, yBitNum)
                        && mbrTuple.getStartTimestamp() <= spatialTemporalPredicate.getStopTimestamp()
                        && mbrTuple.getStopTimestamp() >= spatialTemporalPredicate.getStartTimestamp()) {
                    resultTuples.add(mbrTuple);
                }
            }
        } else {
            // use mbr directly
            for (SpatialIndexNodeTuple mbrTuple : tuples) {
                //SpatialIndexNodeMBRTuple mbrTuple = (SpatialIndexNodeMBRTuple) indexNodeTuple;
                if (SpatialBoundingBox.getOverlappedBoundingBox(mbrTuple.getBoundingBox(), predicateBox) != null
                && mbrTuple.getStartTimestamp() <= spatialTemporalPredicate.getStopTimestamp()
                && mbrTuple.getStopTimestamp() >= spatialTemporalPredicate.getStartTimestamp()) {
                    resultTuples.add(mbrTuple);
                }
            }
        }

        return resultTuples;
    }

    public boolean insert(TrajectorySegmentMeta meta) {
        //TwoLevelGridIndex spatialIndex = this.getIndexTree().getSpatialTwoLevelGridIndex();
        if (getIndexTree().getIndexConfiguration().isUsePreciseSpatialIndex()) {

            /*Set<Long> gridIdList = spatialIndex.toPreciseIndexGridsOptimized(meta.getTrajectoryPointList());
            for (Long gridId: gridIdList) {
                SpatialIndexNodeTuple spatialIndexNodeTuple = new SpatialIndexNodeTuple(meta.getBlockId(), gridId, meta.getDeviceId());
                spatialIndexNodeTuple.setNodeType(NodeType.DATA);
                tuples.add(spatialIndexNodeTuple);
                this.getIndexTree().spatialEntryNum++;
            }*/
            SpatialBoundingBox boundingBox = new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight());
            byte[] signature = SpatialGridSignature.generateSignature(boundingBox, meta.getTrajectoryPointList(),xBitNum, yBitNum);
            SpatialIndexNodeTuple mbrTuple = new SpatialIndexNodeTuple(meta.getBlockId(), meta.getDeviceId(), boundingBox, meta.getStartTimestamp(), meta.getStopTimestamp(), signature);
            mbrTuple.setNodeType(NodeType.DATA);
            tuples.add(mbrTuple);
            this.getIndexTree().spatialEntryNum++;

        } else {
            //gridIdList = spatialIndex.toIndexGrids(new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight()));
            SpatialBoundingBox boundingBox = new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight());
            SpatialIndexNodeTuple mbrTuple = new SpatialIndexNodeTuple(meta.getBlockId(), meta.getDeviceId(), boundingBox, meta.getStartTimestamp(), meta.getStopTimestamp());
            mbrTuple.setNodeType(NodeType.DATA);
            tuples.add(mbrTuple);
            this.getIndexTree().spatialEntryNum++;
        }


        return true;
    }

    private static List<Point> transfer2PointList(List<TrajectoryPoint> trajectoryPointList) {
        List<Point> pointList = new ArrayList<>();
        for (TrajectoryPoint trajectoryPoint : trajectoryPointList) {
            Point point = new Point(trajectoryPoint.getLongitude(), trajectoryPoint.getLatitude());
            pointList.add(point);
        }
        return pointList;
    }


    private boolean isIntersected(SpatialIndexNodeTuple tuple, SpatialTemporalRangeQueryPredicate predicate) {

        long spatialGridId = tuple.getSpatialGridId();
        SpatialBoundingBox predicateBoundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());

        TwoLevelGridIndex spatialIndex = this.getIndexTree().getSpatialTwoLevelGridIndex();
        SpatialBoundingBox tupleBoundingBox = spatialIndex.getBoundingBoxByGridId(spatialGridId);
        SpatialBoundingBox result = spatialIndex.getOverlappedBoundingBox(predicateBoundingBox, tupleBoundingBox);

        if (result == null) {
            return false;
        }

        return true;
    }

    public List<SpatialIndexNodeTuple> getTuples() {
        return tuples;
    }

    public void setTuples(List<SpatialIndexNodeTuple> tuples) {
        this.tuples = tuples;
    }

    @Override
    public String toString() {
        return tuples + "\n ";
    }
}
