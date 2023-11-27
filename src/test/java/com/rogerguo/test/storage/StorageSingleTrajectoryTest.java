package com.rogerguo.test.storage;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.flush.*;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.storage.layer.StorageLayer;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.ChunkIdManager;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-01-04 3:19 PM
 **/
public class StorageSingleTrajectoryTest {

    private static List<TrajectoryPoint> syntheticPoints = SyntheticDataGenerator.generateSyntheticPointsForStorageTest(200, 5, 5);


    @Test
    public void testInsertion() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 55;

        // parameters for the index for immutable chunks
        int indexNodeSize = 512;
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);

        // parameters for the index for head chunks
        int geoHashShiftLength = 20;
        int postingListCapacity = 100;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int objectSize = 2;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SINGLE_TRAJECTORY, spatialShiftNum, timePartitionLength);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 100;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 500;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema);


        int count = 0;
        for (TrajectoryPoint point : syntheticPoints) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 100000 == 0) {
                System.out.println(count);
            }

        }
        seriesStore.stop();

        for (StorageLayerName storageLayerName : storageManager.getStorageLayerHierarchyNameList()) {
            StorageLayer storageLayer = storageManager.getStorageLayerMap().get(storageLayerName);
            System.out.println(storageLayer.printStatus());
        }
        System.out.println(toDiskFlushPolicy.printStatus());
        System.out.println(toS3FlushPolicy.printStatus());
        System.out.println(S3SingleTrajectoryLayoutSchemaTool.printStatus());
    }

    @Test
    public void testIdTemporalQuery() {

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 55;

        // parameters for the index for immutable chunks
        int indexNodeSize = 512;
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable = indexForImmutable.loadAndRebuildIndex();

        // parameters for the index for head chunks
        int geoHashShiftLength = 20;
        int postingListCapacity = 100;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int objectSize = 2;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SINGLE_TRAJECTORY, spatialShiftNum, timePartitionLength);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 100;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 500;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema);

        List<IdTemporalQueryPredicate> predicateList = StorageSpatioTemporalTest.generateRandomIdTemporalQueries(100, 30);
        System.out.println("begin query");
        for (IdTemporalQueryPredicate predicate : predicateList) {
            List<Chunk> result = seriesStore.idTemporalQuery(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());
            //System.out.println(result);
            List<TrajectoryPoint> resultPoints = new ArrayList<>();
            for (Chunk chunk : result) {
                resultPoints.addAll(chunk.getChunk());
            }
            StorageSpatioTemporalTest.verifyIdTemporal(predicate, resultPoints, syntheticPoints);
        }

    }

    @Test
    public void testSpatioTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 55;

        // parameters for the index for immutable chunks
        int indexNodeSize = 512;
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable = indexForImmutable.loadAndRebuildIndex();

        // parameters for the index for head chunks
        int geoHashShiftLength = 20;
        int postingListCapacity = 100;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int objectSize = 2;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SINGLE_TRAJECTORY, spatialShiftNum, timePartitionLength);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 100;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 500;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema);


        // query test
        SpatialTemporalRangeQueryPredicate spatialTemporalRangeQueryPredicate = new SpatialTemporalRangeQueryPredicate(0, 200, new Point(0, 0), new Point(20, 20));
        List<Chunk> fullResult = seriesStore.spatialTemporalRangeQuery(spatialTemporalRangeQueryPredicate.getStartTimestamp(), spatialTemporalRangeQueryPredicate.getStopTimestamp(), new SpatialBoundingBox(spatialTemporalRangeQueryPredicate.getLowerLeft(), spatialTemporalRangeQueryPredicate.getUpperRight()));
        //System.out.println(result);
        List<TrajectoryPoint> resultPointsFull = new ArrayList<>();
        for (Chunk chunk : fullResult) {
            resultPointsFull.addAll(chunk.getChunk());
        }
        StorageSpatioTemporalTest.verifySpatioTemporal(spatialTemporalRangeQueryPredicate, resultPointsFull, syntheticPoints);

        List<SpatialTemporalRangeQueryPredicate> predicateList = StorageSpatioTemporalTest.generateRandomSpatioTemporalQueries(20, 8, 8, 30);
        System.out.println("begin query");
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            List<Chunk> result = seriesStore.spatialTemporalRangeQuery(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            //System.out.println(result);
            List<TrajectoryPoint> resultPoints = new ArrayList<>();
            for (Chunk chunk : result) {
                resultPoints.addAll(chunk.getChunk());
            }
            StorageSpatioTemporalTest.verifySpatioTemporal(predicate, resultPoints, syntheticPoints);
        }
    }

}
