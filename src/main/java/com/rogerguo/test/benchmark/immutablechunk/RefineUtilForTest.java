package com.rogerguo.test.benchmark.immutablechunk;

import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.SpatialTemporalBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.NodeTuple;
import com.rogerguo.test.index.TemporalIndexNodeTuple;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * only for test, in real-world, we fetch block from memory, disk or s3
 * @author yangguo
 * @create 2021-12-29 5:00 PM
 **/
public class RefineUtilForTest {

    public static List<TrajectoryPoint> refineIdTemporal(IdTemporalQueryPredicate predicate, List<NodeTuple> tupleList, List<TrajectorySegmentMeta> metaList) {
        List<TrajectoryPoint> resultList = new ArrayList<>();
        for (NodeTuple tuple : tupleList) {
            String oid = tuple.getBlockId();
            int index = Integer.parseInt(oid);
            TrajectorySegmentMeta meta = metaList.get(index);
            for (TrajectoryPoint point : meta.getTrajectoryPointList()) {
                if (predicate.getDeviceId().equals(point.getOid()) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                    resultList.add(point);
                }
            }
        }

        return resultList;
    }

    public static List<TrajectoryPoint> refineSpatioTemporal(SpatialTemporalRangeQueryPredicate predicate, List<NodeTuple> tupleList, List<TrajectorySegmentMeta> metaList) {
        List<TrajectoryPoint> resultList = new ArrayList<>();
        SpatialBoundingBox boundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        for (NodeTuple tuple : tupleList) {
            String oid = tuple.getBlockId();
            int index = Integer.parseInt(oid);
            TrajectorySegmentMeta meta = metaList.get(index);
            for (TrajectoryPoint point : meta.getTrajectoryPointList()) {
                if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point) && predicate.getStartTimestamp() <= point.getTimestamp()
                && predicate.getStopTimestamp() >= point.getTimestamp()) {
                    resultList.add(point);
                }
            }
        }
        return resultList;
    }

}
