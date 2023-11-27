package com.rogerguo.test.store;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class HeadChunkIndexWithRStartreeTest {

    @Test
    public void updateIndex() {

        HeadChunkIndexWithRStartree index = new HeadChunkIndexWithRStartree(8);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(200, 0.002, 0.001);

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
        }

        System.out.println(index.printStatus());
    }

    @Test
    public void removeFromIndex() {
        HeadChunkIndexWithRStartree index = new HeadChunkIndexWithRStartree(8);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(20, 0.002, 0.001);
        List<Chunk> chunkList = new ArrayList<>();

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
            List<TrajectoryPoint> trajectoryPoints = new ArrayList<>();
            trajectoryPoints.add(point);
            Chunk chunk = new Chunk(point.getOid());
            chunk.setChunk(trajectoryPoints);
            chunkList.add(chunk);
        }

        for (Chunk chunk : chunkList) {
            index.removeFromIndex(chunk);
        }

        System.out.println(index.printStatus());

    }

    @Test
    public void searchForSpatial() {
        HeadChunkIndexWithRStartree index = new HeadChunkIndexWithRStartree(8);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(40, 0.004, 0.004);

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
        }

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(0.0001, 0.0005));
        Set<String> result = index.searchForSpatial(boundingBox);
        System.out.println(result.size());

        // check
        int count = 0;
        for (TrajectoryPoint point : pointList) {
            if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                count++;
            }
        }
        System.out.println("real count: " + count);
        System.out.println(index.printStatus());

    }
}