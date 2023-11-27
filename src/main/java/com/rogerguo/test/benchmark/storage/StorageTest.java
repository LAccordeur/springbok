package com.rogerguo.test.benchmark.storage;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.*;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.*;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.storage.layer.StorageLayer;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.ChunkIdManager;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-01-03 11:59 AM
 **/
public class StorageTest {

    public static void main(String[] args) {
        testParams();
    }

    public static void testParams() {

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

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
        int objectSize = 10000;
        int numOfConnection = 2;
        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 100000;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
            if (count >= 1000000) {
                break;
            }
        }
        seriesStore.stop();

        for (StorageLayerName storageLayerName : storageManager.getStorageLayerHierarchyNameList()) {
            StorageLayer storageLayer = storageManager.getStorageLayerMap().get(storageLayerName);
            System.out.println(storageLayer.printStatus());
        }
        System.out.println(toDiskFlushPolicy.printStatus());
        System.out.println(toS3FlushPolicy.printStatus());
        System.out.println(S3SpatioTemporalLayoutSchemaTool.printStatus());
    }

    public static void testSTRParams() {

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

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
        int objectSize = 1000;
        int numOfConnection = 2;
        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 100000;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
            if (count >= 1000000) {
                break;
            }
        }
        seriesStore.stop();

        for (StorageLayerName storageLayerName : storageManager.getStorageLayerHierarchyNameList()) {
            StorageLayer storageLayer = storageManager.getStorageLayerMap().get(storageLayerName);
            System.out.println(storageLayer.printStatus());
        }
        System.out.println(toDiskFlushPolicy.printStatus());
        System.out.println(toS3FlushPolicy.printStatus());
        System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
    }

    public static void testSingleParams() {

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

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
        int objectSize = 10000;
        int numOfConnection = 2;
        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SINGLE_TRAJECTORY, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 100000;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);
        ChunkIdManager chunkIdManager = new ChunkIdManager(s3LayoutSchema);
        seriesStore.setChunkIdManager(chunkIdManager);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
            if (count >= 1000000) {
                break;
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

    public static void testInsertion() {

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

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
        int objectSize = 10000;
        int numOfConnection = 2;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 100000;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 100000 == 0) {
                System.out.println(count);
            }
            if (count >= 10000000) {
                break;
            }
        }
        seriesStore.stop();
    }

    public static void testIdTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

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
        int objectSize = 10000;
        int numOfConnection = 2;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 100000;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);
        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        // query test
        IdTemporalQueryPredicate idTemporalQueryPredicate = new IdTemporalQueryPredicate(1372636853, 1372637000, "20000380");

        System.out.println("begin query");
        List<Chunk> result = seriesStore.idTemporalQuery(idTemporalQueryPredicate.getDeviceId(), idTemporalQueryPredicate.getStartTimestamp(), idTemporalQueryPredicate.getStopTimestamp());
        System.out.println(result);
        List<TrajectoryPoint> resultPoints = new ArrayList<>();
        for (Chunk chunk : result) {
            resultPoints.addAll(chunk.getChunk());
        }
        verifyIdTemporal(idTemporalQueryPredicate, resultPoints);
    }

    public static void testSpatioTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

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
        int objectSize = 10000;
        int numOfConnection = 2;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 100000;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);
        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        // query test
        SpatialTemporalRangeQueryPredicate spatialTemporalRangeQueryPredicate = new SpatialTemporalRangeQueryPredicate(1372636853, 1372637000, new Point(-8.6104, 41.1407), new Point(-8.6102, 41.1408));

        System.out.println("begin query");
        List<Chunk> result = seriesStore.spatialTemporalRangeQuery(spatialTemporalRangeQueryPredicate.getStartTimestamp(), spatialTemporalRangeQueryPredicate.getStopTimestamp(), new SpatialBoundingBox(spatialTemporalRangeQueryPredicate.getLowerLeft(), spatialTemporalRangeQueryPredicate.getUpperRight()));
        System.out.println(result);
        List<TrajectoryPoint> resultPoints = new ArrayList<>();
        for (Chunk chunk : result) {
            resultPoints.addAll(chunk.getChunk());
        }
        verifySpatioTemporal(spatialTemporalRangeQueryPredicate, resultPoints);
    }

    public static void verifyIdTemporal(IdTemporalQueryPredicate predicate, List<TrajectoryPoint> queryResult) {

        int matchCount = 0;
        List<TrajectoryPoint> realResult = new ArrayList<>();

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            if (count % 100000 == 0) {
                System.out.println(count);
            }
            if (count >= 10000000) {
                break;
            }

            if (predicate.getDeviceId().equals(point.getOid()) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                realResult.add(point);
                for (TrajectoryPoint queryPoint : queryResult) {
                    if (queryPoint.getOid().equals(point.getOid()) && queryPoint.getTimestamp() == point.getTimestamp()) {
                        matchCount++;
                    }
                }
            }
        }

        System.out.println("real count: " + realResult.size());
        System.out.println("match count: " + matchCount);
    }

    public static void verifySpatioTemporal(SpatialTemporalRangeQueryPredicate predicate, List<TrajectoryPoint> queryResult) {
        int matchCount = 0;
        List<TrajectoryPoint> realResult = new ArrayList<>();

        SpatialBoundingBox predicateBoundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            if (count % 100000 == 0) {
                System.out.println(count);
            }
            if (count >= 10000000) {
                break;
            }

            if (SpatialBoundingBox.checkBoundingBoxContainPoint(predicateBoundingBox, point) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                realResult.add(point);
                for (TrajectoryPoint queryPoint : queryResult) {
                    if (queryPoint.getLongitude() == point.getLongitude() && queryPoint.getLatitude() == point.getLatitude() && queryPoint.getTimestamp() == point.getTimestamp()) {
                        matchCount++;
                    }
                }
            }
        }

        System.out.println("real count: " + realResult.size());
        System.out.println("match count: " + matchCount);
    }

}
