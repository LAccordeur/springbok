package com.rogerguo.test.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.util.TreePrinter;
import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.aws.AWSS3Driver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.util.TrajectorySimulator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2021-10-06 8:31 PM
 **/
public class SimpleEvaluation {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        /*delete("flush-test-1111");
        delete("bucket-for-index-20101010");*/
        //appendDataToS3DirectlyTest();
        appendDataForInMemTest();
        //idTemporalQueryTest();
    }

    public static void appendDataForInMemTest() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 400; i++) {
            List<TrajectoryPoint> trajectoryPointList = TrajectorySimulator.nextSyntheticTrajectoryPointBatch(100000);
            for (TrajectoryPoint point : trajectoryPointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }
        //seriesStore.stop();
        //new TreePrinter(seriesStore.getIndexForImmutableChunks()).print(System.out);
        long stop = System.currentTimeMillis();
        System.out.println("insertion takes " + (stop - start) + " ms");
    }

    public static void appendDataWithOptimizedS3FlushTest() {
        SeriesStore seriesStore = SeriesStore.initNewStoreWithOptimizedS3FlushForTest();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) {
            List<TrajectoryPoint> trajectoryPointList = TrajectorySimulator.nextSyntheticTrajectoryPointBatch(20);
            for (TrajectoryPoint point : trajectoryPointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }
        seriesStore.stop();
        //new TreePrinter(seriesStore.getIndexForImmutableChunks()).print(System.out);
        long stop = System.currentTimeMillis();
        System.out.println("insertion takes " + (stop - start) + " ms");
    }

    public static void appendDataTest() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForTest();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) {
            List<TrajectoryPoint> trajectoryPointList = TrajectorySimulator.nextSyntheticTrajectoryPointBatch(20);
            for (TrajectoryPoint point : trajectoryPointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }
        seriesStore.stop();
        //new TreePrinter(seriesStore.getIndexForImmutableChunks()).print(System.out);
        long stop = System.currentTimeMillis();
        System.out.println("insertion takes " + (stop - start) + " ms");
    }

    public static void appendDataToS3DirectlyTest() {

        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        ObjectStoreDriver objectStoreDriver = new ObjectStoreDriver(bucketName, region);
        int chunkSize = 1000;
        int id = 0;
        String device = "device_test";
        List<TrajectoryPoint> trajectoryPointList = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 2000 * 20; i++) {
            trajectoryPointList.add(TrajectorySimulator.nextSyntheticTrajectoryPoint(device));
            if ((i + 1) % chunkSize == 0) {
                Chunk chunk = new Chunk(device, String.valueOf(id));
                chunk.setChunk(trajectoryPointList);
                id = id + 1;
                Block block = new Block(ChunkIdManager.generateStringBlockId(chunk), Chunk.serialize(chunk));
                try {
                    objectStoreDriver.flush(block.getBlockId(), objectMapper.writeValueAsString(block));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                trajectoryPointList.clear();
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("insertion takes " + (stop - start) + " ms");
    }

    public static void idTemporalQueryTest() {
        SeriesStore seriesStore = SeriesStore.initExistedStoreForTest();
        List<Chunk> result = seriesStore.idTemporalQuery("device_1", 0, 3);
        //System.out.println(result);
    }

    public static void spatialTemporalQueryTest() {
        SeriesStore seriesStore = SeriesStore.initExistedStoreForTest();
        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(3, 3));
        List<Chunk> result = seriesStore.spatialTemporalRangeQuery(0, 3, spatialBoundingBox);
        System.out.println(result);
    }

    public static void delete(String bucketName) {
        Region region = Region.AP_EAST_1;
        S3Client s3Client = S3Client.builder().region(region).build();

        AWSS3Driver.deleteBucket(s3Client, bucketName);
    }

}
