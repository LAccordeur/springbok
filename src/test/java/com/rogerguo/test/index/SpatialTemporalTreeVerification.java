package com.rogerguo.test.index;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.SpatialTemporalBoundingBox;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.index.util.IndexTupleGenerator;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2021-12-28 8:47 PM
 **/
public class SpatialTemporalTreeVerification {

    private List<TrajectorySegmentMeta> segmentMetaList1000 = SyntheticDataGenerator.generateRandomDistributedIndexEntries(1000, 1, 1);

    private List<TrajectorySegmentMeta> segmentMetaList10000 = SyntheticDataGenerator.generateRandomDistributedIndexEntries(10000, 1, 1);

    @Test
    public void idTemporalQuery() {
        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        int nodeSize = 4;
        boolean lazyUpdate = true;
        IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, lazyUpdate, bucketName, rootDirnameInBucket, region, true, true);
        SpatialTemporalTree indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

        for (TrajectorySegmentMeta meta : segmentMetaList10000) {
            indexTree.insert(meta);
        }
        System.out.println(indexTree.printStatus());

        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(1, 80, "40");
        List<NodeTuple> result = indexTree.searchForIdTemporal(predicate);
        System.out.println(result);
        verifyIdTemporal(predicate, segmentMetaList10000, result);
    }

    @Test
    public void spatioTemporalQuery() {
        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        int nodeSize = 4;
        boolean lazyUpdate = true;
        IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, lazyUpdate, bucketName, rootDirnameInBucket, region, true, true);
        SpatialTemporalTree indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

        for (TrajectorySegmentMeta meta : segmentMetaList10000) {
            indexTree.insert(meta);
        }

        SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(10, 19, new Point(0, 0), new Point(8, 8));
        List<NodeTuple> result = indexTree.searchForSpatialTemporal(predicate);
        System.out.println(result);
        verifySpatioTemporal(predicate, segmentMetaList10000, result);
        System.out.println();
    }

    public void verifyIdTemporal(IdTemporalQueryPredicate predicate, List<TrajectorySegmentMeta> inputSegmentMetaList, List<NodeTuple> queryResult) {

        List<TrajectorySegmentMeta> result = new ArrayList<>();
        for (TrajectorySegmentMeta meta : inputSegmentMetaList) {
            if (meta.getDeviceId().equals(predicate.getDeviceId()) && meta.getStartTimestamp() <= predicate.getStopTimestamp() && meta.getStopTimestamp() >= predicate.getStartTimestamp()) {
                result.add(meta);
            }
        }

        int matchNum = 0;

        for (NodeTuple tuple : queryResult) {
            for (TrajectorySegmentMeta meta : result) {
                if (meta.getBlockId().equals(tuple.getBlockId())) {
                    matchNum = matchNum + 1;
                }
            }
        }

        System.out.println("actual num: " + result.size());
        System.out.println("matched num: " + matchNum);
    }

    public void verifySpatioTemporal(SpatialTemporalRangeQueryPredicate predicate, List<TrajectorySegmentMeta> inputSegmentMetaList, List<NodeTuple> queryResult) {

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        List<TrajectorySegmentMeta> result = new ArrayList<>();
        for (TrajectorySegmentMeta meta : inputSegmentMetaList) {
            boolean timePredicate = meta.getStartTimestamp() <= predicate.getStopTimestamp() && meta.getStopTimestamp() >= predicate.getStartTimestamp();
            boolean spatialPredicate = SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, meta.getLowerLeft()) || SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, meta.getUpperRight())
                    || SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, new Point(meta.getLowerLeft().getLongitude(), meta.getUpperRight().getLatitude()))
                    || SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, new Point(meta.getUpperRight().getLongitude(), meta.getLowerLeft().getLatitude()));
            if (timePredicate && spatialPredicate) {
                result.add(meta);
            }
        }

        int matchNum = 0;
        for (NodeTuple tuple : queryResult) {
            for (TrajectorySegmentMeta meta : result) {
                if (meta.getBlockId().equals(tuple.getBlockId())) {
                    matchNum = matchNum + 1;
                }
            }
        }

        System.out.println("actual num: " + result.size());
        System.out.println("matched num: " + result.size());
    }

}
