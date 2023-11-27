package com.rogerguo.test.store;

import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.util.TrajectorySimulator;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.List;

public class SingleSeriesTest {

    /**
     * test with only one chunk
     */
    @Test
    public void appendPoint() {
        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";

        tieredCloudStorageManager.initLayersStructure(TieredCloudStorageManager.getDefaultStorageConfiguration());
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(4);
        HeadChunkIndexWithTwoLevelGrid indexForHeadChunks = new HeadChunkIndexWithTwoLevelGrid();
        SingleSeries singleSeries = new SingleSeries("device_0", 4, tieredCloudStorageManager, new ChunkIdManager(), indexForImmutable, indexForHeadChunks);

        List<TrajectoryPoint> pointList = TrajectorySimulator.generateSyntheticTrajectory(1, 10);
        for (TrajectoryPoint point : pointList) {
            singleSeries.appendPoint(point);
        }

        System.out.println(singleSeries);
    }

    /**
     * test with multiple chunks
     */
    @Test
    public void appendPoint2() {
/*        ImmutableMemoryRegion immutableMemoryRegion = new ImmutableMemoryRegion(new DiskDriver("data-test"));
        SingleSeries singleSeries = new SingleSeries("device_0", 4, immutableMemoryRegion, new ChunkIdManager(), null, null);

        List<TrajectoryPoint> pointList = TrajectorySimulator.generateTrajectory(1, 10);
        for (int i = 0; i < pointList.size(); i++) {
            singleSeries.appendPoint(pointList.get(i));
        }

        System.out.println(singleSeries);*/
    }
}