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

public class HeadChunkIndexWithRStartreeMBRTest {

    @Test
    public void updateIndex() {
        HeadChunkIndexWithRStartreeMBR index = new HeadChunkIndexWithRStartreeMBR(16);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(200, 0.002, 0.001);

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
            TrajectoryPoint point1 = new TrajectoryPoint(point.getOid(), 1, point.getLongitude()+0.1, point.getLatitude());
            index.updateIndex(point1);
        }

        System.out.println(index.printStatus());
    }

    @Test
    public void removeFromIndex() {
        HeadChunkIndexWithRStartreeMBR index = new HeadChunkIndexWithRStartreeMBR( 4);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(20, 0.002, 0.001);
        List<Chunk> chunkList = new ArrayList<>();

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
            Chunk chunk = new Chunk(point.getOid());
            chunkList.add(chunk);
        }

        for (Chunk chunk : chunkList) {
            index.removeFromIndex(chunk);
            System.out.println(index.printStatus());
        }

        System.out.println(index.printStatus());

    }

    @Test
    public void searchForSpatial() {

        HeadChunkIndexWithRStartreeMBR index = new HeadChunkIndexWithRStartreeMBR( 4);
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(400, 0.004, 0.004);
        List<Chunk> chunkList = new ArrayList<>();

        for (TrajectoryPoint point : pointList) {
            index.updateIndex(point);
            Chunk chunk = new Chunk(point.getOid());
            chunkList.add(chunk);
        }

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(0.003, 0.003));
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

    @Test
    public void extendBox() {
        HeadChunkIndexWithRStartreeMBR index = new HeadChunkIndexWithRStartreeMBR(8);
        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(1, 34), new Point(2, 66));
        System.out.println(index.extendBoundingBoxByPoint(boundingBox, new Point(99, 99)));
    }
}