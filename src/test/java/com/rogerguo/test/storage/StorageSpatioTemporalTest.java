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
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import com.rogerguo.test.util.ZCurve;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.*;

/**
 * @author yangguo
 * @create 2022-01-04 12:32 PM
 **/
public class StorageSpatioTemporalTest {

    private static List<TrajectoryPoint> syntheticPoints = SyntheticDataGenerator.generateSyntheticPointsForStorageTest(200, 20, 20);

    @Test
    public void testSpatialEncoding() {
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateSyntheticPointsForStorageTest(1, 20, 20);

        Map<Long, List<TrajectoryPoint>> partitionPoints = new HashMap<>();
        ZCurve zCurve = new ZCurve();
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);
        for (TrajectoryPoint point : pointList) {
            long spatialEncoding = zCurve.getCurveValue(normalizer.normalizeDimensionX(point.getLongitude()), normalizer.normalizeDimensionY(point.getLatitude()));
            long spatialPartitionId = S3SpatioTemporalLayoutSchemaTool.generateSpatialPartitionId(spatialEncoding, 36);

            if (partitionPoints.containsKey(spatialPartitionId)) {
                partitionPoints.get(spatialPartitionId).add(point);
            } else {
                List<TrajectoryPoint> points = new ArrayList<>();
                points.add(point);
                partitionPoints.put(spatialPartitionId, points);
            }

            System.out.print("point (" + point.getLongitude() + ", " + point.getLatitude() + "), partition id: ");
            System.out.println(spatialPartitionId);
        }

        for (long key : partitionPoints.keySet()) {
            System.out.println(key);
            System.out.println(partitionPoints.get(key));
            System.out.println("size: " + partitionPoints.get(key).size());
            System.out.println();
        }
    }

    @Test
    public void testInsertion() {

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

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
        int objectSize = 100;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, spatialShiftNum, timePartitionLength);
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
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

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
        System.out.println(S3SpatioTemporalLayoutSchemaTool.printStatus());
    }

    @Test
    public void testIdTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

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
        int objectSize = 100;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, spatialShiftNum, timePartitionLength);
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
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        // query test
        IdTemporalQueryPredicate idTemporalQueryPredicate = new IdTemporalQueryPredicate(0, 40, "3");

        List<IdTemporalQueryPredicate> predicateList = generateRandomIdTemporalQueries(50, 66);

        System.out.println("begin query");
        for (IdTemporalQueryPredicate predicate : predicateList) {
            List<Chunk> result = seriesStore.idTemporalQuery(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());
            System.out.println(result);
            List<TrajectoryPoint> resultPoints = new ArrayList<>();
            for (Chunk chunk : result) {
                resultPoints.addAll(chunk.getChunk());
            }
            verifyIdTemporal(predicate, resultPoints, syntheticPoints);
        }
    }

    @Test
    public void testSpatioTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

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
        int objectSize = 100;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, spatialShiftNum, timePartitionLength);
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
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        // query test
        SpatialTemporalRangeQueryPredicate spatialTemporalRangeQueryPredicate = new SpatialTemporalRangeQueryPredicate(0, 22, new Point(0, 0), new Point(20, 20));
        List<Chunk> fullResult = seriesStore.spatialTemporalRangeQuery(spatialTemporalRangeQueryPredicate.getStartTimestamp(), spatialTemporalRangeQueryPredicate.getStopTimestamp(), new SpatialBoundingBox(spatialTemporalRangeQueryPredicate.getLowerLeft(), spatialTemporalRangeQueryPredicate.getUpperRight()));
        //System.out.println(result);
        List<TrajectoryPoint> resultPointsFull = new ArrayList<>();
        for (Chunk chunk : fullResult) {
            resultPointsFull.addAll(chunk.getChunk());
        }
        verifySpatioTemporal(spatialTemporalRangeQueryPredicate, resultPointsFull, syntheticPoints);

        List<SpatialTemporalRangeQueryPredicate> predicateList = generateRandomSpatioTemporalQueries(100, 8, 8, 30);
        System.out.println("begin query");
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            List<Chunk> result = seriesStore.spatialTemporalRangeQuery(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            //System.out.println(result);
            List<TrajectoryPoint> resultPoints = new ArrayList<>();
            for (Chunk chunk : result) {
                resultPoints.addAll(chunk.getChunk());
            }
            verifySpatioTemporal(predicate, resultPoints, syntheticPoints);
        }
    }

    public static List<IdTemporalQueryPredicate> generateRandomIdTemporalQueries(int size, int timeLength) {
        List<IdTemporalQueryPredicate> predicateList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < size; i++) {
            String randomId = String.valueOf(random.nextInt(25));
            int randomT = random.nextInt(200);
            IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(randomT, randomT + timeLength, randomId);
            predicateList.add(predicate);
        }

        return predicateList;
    }

    public static List<SpatialTemporalRangeQueryPredicate> generateRandomSpatioTemporalQueries(int size, int xLength, int yLength, int timeLength) {
        List<SpatialTemporalRangeQueryPredicate> predicateList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < size; i++) {
            int randomX = random.nextInt(5);
            int randomY = random.nextInt(5);
            int randomT = random.nextInt(200);
            SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(randomT, randomT + timeLength, new Point(randomX, randomY), new Point(randomX+xLength, randomY+yLength));
            predicateList.add(predicate);
        }

        return predicateList;
    }

    public static void verifyIdTemporal(IdTemporalQueryPredicate predicate, List<TrajectoryPoint> queryResult, List<TrajectoryPoint> dataPoints) {

        int matchCount = 0;
        List<TrajectoryPoint> realResult = new ArrayList<>();

        int count = 0;
        for (TrajectoryPoint point : dataPoints) {
            count++;
            if (count % 100000 == 0) {
                System.out.println(count);
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

    public static void verifySpatioTemporal(SpatialTemporalRangeQueryPredicate predicate, List<TrajectoryPoint> queryResult, List<TrajectoryPoint> dataPoints) {
        int matchCount = 0;
        List<TrajectoryPoint> realResult = new ArrayList<>();

        SpatialBoundingBox predicateBoundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());

        int count = 0;
        for (TrajectoryPoint point : dataPoints) {
            count++;
            if (count % 100000 == 0) {
                System.out.println(count);
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
