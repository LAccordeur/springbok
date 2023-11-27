package com.rogerguo.test.test;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.StorageConfiguration;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.storage.flush.ToDiskFlushPolicy;
import com.rogerguo.test.storage.flush.ToS3FlushPolicy;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2022-05-17 11:30 AM
 **/
public class StorageSpatioTemporalTest {

    // dimension normalizer
    static DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

    // parameters for in-memory chunk size (the max number of points in a chunk)
    static int maxChunkSize = 200;
    // parameters for the index for immutable chunks
    static int indexNodeSize = 1024;
    static Region region = Region.AP_EAST_1;
    static String bucketName = "flush-test-1111-from-data";
    static String rootDirnameInBucket = "index-test";
    static boolean lazyParentUpdate = true;
    static boolean preciseSpatialIndex = true;
    static boolean enableSpatialIndex = true;
    static IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);

    // parameters for the index for head chunks
    static int geoHashShiftLength = 12;
    static int postingListCapacity = 50;

    // parameters for tiered storage
    static String bucketNameFrStorage = "flush-test-1111-from-data";
    static Region regionForStorage = Region.AP_EAST_1;
    static int objectSize = 2000;
    static int numOfConnection = 1;
    static int s3TimePartition = 1000 * 60 * 60 * 24;
    static int s3SpatialPartition= 20;
    static S3LayoutSchemaName s3LayoutSchemaName = S3LayoutSchemaName.SPATIO_TEMPORAL;
    static int flushBlockNumThresholdForMem = 10000;
    static int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
    static int flushBlockNumThresholdForDisk = 100000;
    static int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
    static S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
    static String pathNameForDiskTier = "/home/yangguo/Data/flush-test";

    static String timestampModeForChunkId = "from-data";

    public static void main(String[] args) {
        ingestData();

        /*SeriesStore seriesStore = constructStore();

        long start = System.currentTimeMillis();
        System.out.println("begin query");
        long startTime = 1372636853000L;
        long endTime = startTime + 60 * 60 * 6 * 1000;
        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(-8.610291, 41.140746), new Point(-8.610291 + 0.01, 41.140746 + 0.01));
        List<TrajectoryPoint> result = seriesStore.spatialTemporalRangeQueryWithRefinement(startTime, endTime, boundingBox);
        System.out.println(result.size());
        long stop = System.currentTimeMillis();
        System.out.println("time: " + (stop - start) + " ms");*/
        //generateTestSTRangeQueries(10);

        verifyRange();
        System.out.println("finish");

    }

/*    public static void testBug() {
        // fixed: out-of-order block overwrite existing S3 objects
        SeriesStore seriesStore = constructStore();

        List<SpatialTemporalRangeQueryPredicate> predicateList = generateTestSTRangeQueries(10);
        SpatialTemporalRangeQueryPredicate predicate = predicateList.get(1);
            long start = System.currentTimeMillis();
            System.out.println("begin query");

            SpatialBoundingBox boundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
            List<TrajectoryPoint> result = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), boundingBox);
            System.out.println(result.size());

            verifyResult(result, predicate);
            long stop = System.currentTimeMillis();
            System.out.println("time: " + (stop - start) + " ms");

    }*/

    public static void verifyIdTemporal() {
        SeriesStore seriesStore = constructStore();

        List<IdTemporalQueryPredicate> predicateList = generateTestIdTemporalQueries(10);

        for (IdTemporalQueryPredicate predicate : predicateList) {
            long start = System.currentTimeMillis();
            System.out.println("begin query");


            List<TrajectoryPoint> result = seriesStore.idTemporalQueryWithRefinement(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());
            System.out.println(result.size());

            verifyIdTemporalResult(result, predicate);
            long stop = System.currentTimeMillis();
            System.out.println("time: " + (stop - start) + " ms");
        }
    }

    public static void verifyRange() {
        SeriesStore seriesStore = constructStore();

        List<SpatialTemporalRangeQueryPredicate> predicateList = generateTestSTRangeQueries(10);
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            long start = System.currentTimeMillis();
            System.out.println("begin query");

            SpatialBoundingBox boundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
            List<TrajectoryPoint> result = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), boundingBox);
            System.out.println(result.size());

            verifyRangeResult(result, predicate);
            long stop = System.currentTimeMillis();
            System.out.println("time: " + (stop - start) + " ms");
            System.out.println("\n\n");
        }
    }

    public static SeriesStore constructStore() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = new StorageConfiguration(regionForStorage, bucketNameFrStorage, flushBlockNumThresholdForMem, flushTimeThresholdForMem, flushTimeThresholdForDisk, flushBlockNumThresholdForDisk, s3LayoutSchema, pathNameForDiskTier);
        tieredCloudStorageManager.setStorageLayers(storageConfiguration);

        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable = indexForImmutable.loadAndRebuildIndex();

        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, timestampModeForChunkId);

        return seriesStore;
    }

    public static void ingestData() {


        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        //indexForImmutable = indexForImmutable.loadAndRebuildIndex();


        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);


        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,pathNameForDiskTier, flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, timestampModeForChunkId);

        // ingest
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");

        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }

        }

        seriesStore.flushDataToS3();
        indexForImmutable.serializeAndFlushIndex();

    }

    public static void verifyIdTemporalResult(List<TrajectoryPoint> resultFromSpringbok, IdTemporalQueryPredicate predicate) {
        int matchedCount = 0;
        int actualCount = 0;

        System.out.println(predicate);

        TrajectoryPoint point = null;
        int count = 0;
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");

        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            if (predicate.getDeviceId().equals(point.getOid()) && predicate.getStartTimestamp() <= point.getTimestamp()
                    && predicate.getStopTimestamp() >= point.getTimestamp()) {
                actualCount = actualCount + 1;

                if (containPoint(resultFromSpringbok, point)) {
                    matchedCount++;
                } else {
                    System.out.println(point);
                }
            }

            if (count > 15000000) {
                break;
            }
        }

        System.out.println("actual count: " + actualCount);
        System.out.println("matched count: " + matchedCount);
        System.out.println();
    }

    public static void verifyRangeResult(List<TrajectoryPoint> resultFromSpringbok, SpatialTemporalRangeQueryPredicate predicate) {
        int matchedCount = 0;
        int actualCount = 0;

        System.out.println(predicate);

        TrajectoryPoint point = null;
        int count = 0;
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");
        SpatialBoundingBox boundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point) && predicate.getStartTimestamp() <= point.getTimestamp()
                    && predicate.getStopTimestamp() >= point.getTimestamp()) {
                actualCount = actualCount + 1;

                if (containPoint(resultFromSpringbok, point)) {
                    matchedCount++;
                } else {
                    System.out.println(point);
                }
            }

            if (count > 15000000) {
                break;
            }
        }

        System.out.println("actual count: " + actualCount);
        System.out.println("matched count: " + matchedCount);
        System.out.println();
    }

    public static boolean containPoint(List<TrajectoryPoint> points, TrajectoryPoint checkPoint) {
        for (TrajectoryPoint point : points) {
            if (point.getOid().equals(checkPoint.getOid())
                && point.getTimestamp() == checkPoint.getTimestamp()
                && Math.abs(point.getLongitude() - checkPoint.getLongitude()) < 0.000000001
                && Math.abs(point.getLatitude() - checkPoint.getLatitude()) < 0.000000001) {
                return true;
            }
        }
        return false;
    }

    public static List<SpatialTemporalRangeQueryPredicate> generateTestSTRangeQueries(int num) {

        List<SpatialTemporalRangeQueryPredicate> predicateList = new ArrayList<>();

        List<Integer> randomIndexList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < num; i++) {
            randomIndexList.add(random.nextInt(10000000));
        }

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");

        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
            if (randomIndexList.contains(count)) {
                Point upperRight = new Point(point.getLongitude() + 0.05, point.getLatitude() + 0.05);
                SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(point.getTimestamp(), point.getTimestamp() + (60 * 60 * 6 * 1000), point, upperRight);
                predicateList.add(predicate);
                System.out.println(predicate);
            }

            if (count > 10000000) {
                break;
            }

        }

        return predicateList;
    }

    public static List<IdTemporalQueryPredicate> generateTestIdTemporalQueries(int num) {
        List<IdTemporalQueryPredicate> predicateList = new ArrayList<>();

        List<Integer> randomIndexList = new ArrayList<>();
        Random random = new Random(1);
        for (int i = 0; i < num; i++) {
            randomIndexList.add(random.nextInt(10000000));
        }

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv");

        TrajectoryPoint point = null;
        int count = 0;

        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
            if (randomIndexList.contains(count)) {

                IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(point.getTimestamp(), point.getTimestamp() + (60 * 60 * 6 * 1000), point.getOid());
                predicateList.add(predicate);
                System.out.println(predicate);
            }

            if (count > 10000000) {
                break;
            }

        }

        return predicateList;
    }
}
