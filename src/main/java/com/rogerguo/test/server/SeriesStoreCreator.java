package com.rogerguo.test.server;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.recovery.LeafNodeStatusRecorder;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.storage.flush.ToDiskFlushPolicy;
import com.rogerguo.test.storage.flush.ToS3FlushPolicy;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import com.rogerguo.test.store.StoreConfig;
import software.amazon.awssdk.regions.Region;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.common.Point;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * @author yangguo
 * @create 2022-06-25 4:21 PM
 **/
public class SeriesStoreCreator {

    private static int objectSize = StoreConfig.OBJECT_SIZE;

    private static int numOfConnection = 1;

    public static void main(String[] args) {

        testInsertion();
        //testQueryFromS3();
        //testQueryFromS3WithDiskCacheFill();
        //testQueryFromDisk();
    }

    static void testInsertion() {
        String dataFile = "/home/ubuntu/data/porto_data_v1_1000w.csv";
        //String dataFile = "/home/ubuntu/data1/porto_data_v1_45x.csv";
        long startTime = System.currentTimeMillis();
        SeriesStore seriesStore = SeriesStoreCreator.createAndFillSeriesStore(dataFile, S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-springbok", false);
        long stopTime = System.currentTimeMillis();

        System.out.println("finish init and insertion");
        System.out.println("total insertion time: " + (stopTime - startTime));
    }

    static void testQueryFromS3() {
        long totalTime = 0;
        SeriesStore seriesStore = SeriesStoreCreator.startExistedSeriesStore(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-springbok", true);
        String queryFileName = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_24h_01.query";
        List<SpatialTemporalRangeQueryPredicate> predicateList = getSpatialTemporalRangeQueriesFromQueryFile(queryFileName);
        int count = 0;
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            long startTime = System.currentTimeMillis();
            List<TrajectoryPoint> resultPoints = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            long stopTime = System.currentTimeMillis();
            System.out.println("result points num: " + resultPoints.size() + ", time: " + (stopTime - startTime));
            totalTime += (stopTime - startTime);
            System.out.println("\n");
            count++;
            if (count > 50) {
                break;

            }
        }
        System.out.println("total query time: " + totalTime);

    }

    static void testQueryFromS3WithDiskCacheFill() {
        SeriesStore seriesStore = SeriesStoreCreator.startExistedSeriesStoreWithDiskCacheFill(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-springbok", true, 0, 1);
        String queryFileName = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_24h_01.query";
        List<SpatialTemporalRangeQueryPredicate> predicateList = getSpatialTemporalRangeQueriesFromQueryFile(queryFileName);
        int count = 0;
        long totalTime = 0;
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            long startTime = System.currentTimeMillis();
            List<TrajectoryPoint> resultPoints = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            long stopTime = System.currentTimeMillis();
            totalTime += (stopTime - startTime);
            System.out.println("result points num: " + resultPoints.size() + ", time: " + (stopTime - startTime));
            System.out.println("\n");
            count++;
            if (count > 50) {
                break;

            }
        }
        System.out.println("total query time: " + totalTime);

    }

    static void testQueryFromDisk() {
        //String dataFile = "/home/ubuntu/data/porto_data_v1_1000w.csv";
        String dataFile = "/home/ubuntu/data/porto_data_v1.csv";
        SeriesStore seriesStore = SeriesStoreCreator.createAndFillSeriesStore(dataFile, S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-springbok", false);

        System.out.println(seriesStore.getIndexForImmutableChunks().printStatus());

        String queryFileName = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_24h_01.query";
        List<SpatialTemporalRangeQueryPredicate> predicateList = getSpatialTemporalRangeQueriesFromQueryFile(queryFileName);
        long totalColdTime = 0;
        long totalWarmTime = 0;
        int count = 0;
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            long startTime = System.currentTimeMillis();
            List<TrajectoryPoint> resultPoints = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            long stopTime = System.currentTimeMillis();
            System.out.println("result points num: " + resultPoints.size() + ", time: " + (stopTime - startTime));
            totalColdTime += (stopTime - startTime);
            startTime = System.currentTimeMillis();
            List<TrajectoryPoint> resultPoints2 = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            stopTime = System.currentTimeMillis();
            System.out.println("result points num: " + resultPoints2.size() + ", time (warm): " + (stopTime - startTime));
            totalWarmTime += (stopTime - startTime);

            System.out.println("\n");
            count++;
            if (count > 50) {
                break;

            }
        }

        System.out.println("total cold time: " + totalColdTime);
        System.out.println("total warm time: " + totalWarmTime);

    }


    static void testRecovery() {
        DiskDriver diskDriver = new DiskDriver("/home/ubuntu/data/recovery-test");
        LeafNodeStatusRecorder leafNodeStatusRecorder = new LeafNodeStatusRecorder(diskDriver);


        SeriesStore seriesStore = SeriesStoreCreator.createEmptySeriesStoreWithRecoveryAndDiskFlush(S3LayoutSchemaName.SINGLE_TRAJECTORY, "springbok-store-csv-eva", leafNodeStatusRecorder);
        TrajectoryPoint point = null;
        int count = 0;
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/ubuntu/dataset/porto_data_v1.csv");
        long start = System.currentTimeMillis();
        while ((point =portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            seriesStore.appendSeriesPoint(point);

            count++;
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
        }

        seriesStore.stop();
        //seriesStore.flushDataToDisk();
        long stop = System.currentTimeMillis();
        System.out.println("insertion takes " + (stop - start) + " ms");
    }

    static SeriesStore createEmptySeriesStore(S3LayoutSchemaName s3LayoutSchemaName, String bucketName, boolean flushToS3) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = StoreConfig.IMMUTABLE_CHUNK_CAPACITY;
        System.out.println("chunk capacity: " + maxChunkSize);
        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.US_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.US_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/ubuntu/data/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");



        return seriesStore;
    }

    static SeriesStore startExistedSeriesStore(S3LayoutSchemaName s3LayoutSchemaName, String bucketName, boolean flushToS3) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.US_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        long indexStart = System.currentTimeMillis();
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf).loadAndRebuildIndex();
        long indexStop = System.currentTimeMillis();
        System.out.println("index rebuild time: " + (indexStop - indexStart));
        System.out.println(indexForImmutable.printStatus());

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.US_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/ubuntu/data/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");



        return seriesStore;
    }

    static SeriesStore startExistedSeriesStoreWithDiskCacheFill(S3LayoutSchemaName s3LayoutSchemaName, String bucketName, boolean flushToS3, int cacheFillMode, double cacheFillPercent) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.US_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        long indexStart = System.currentTimeMillis();
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf).loadAndRebuildIndex();
        long indexStop = System.currentTimeMillis();
        System.out.println("index rebuild time: " + (indexStop - indexStart));
        System.out.println(indexForImmutable.printStatus());

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.US_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/ubuntu/data/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);
        storageManager.fillDiskLayerCacheDataFromS3(cacheFillMode, cacheFillPercent);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");



        return seriesStore;
    }


    static SeriesStore createEmptySeriesStoreWithRecoveryAndDiskFlush(S3LayoutSchemaName s3LayoutSchemaName, String bucketName, LeafNodeStatusRecorder recorder) {
        boolean flushToS3 = true;

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.US_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex, true);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable.setLeafNodeStatusRecorder(recorder);

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.US_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        toDiskFlushPolicy.setLeafNodeStatusRecorder(recorder);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/ubuntu/data/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

        return seriesStore;
    }

    static SeriesStore createAndFillSeriesStore(String dataFile, S3LayoutSchemaName s3LayoutSchemaName, String bucketName, boolean flushToS3) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.US_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        //indexForImmutable = null;   // disable index

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.US_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        //int flushBlockNumThresholdForMem = 10000000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/ubuntu/data1/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFile);

        TrajectoryPoint point = null;
        long count = 0;
        while ((point = portoTaxiRealData.nextPoint()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }

        }
        if (flushToS3) {
            seriesStore.flushDataToS3();
        } else {
            seriesStore.flushDataToDisk();
        }
        long indexStart = System.currentTimeMillis();
        indexForImmutable.close();
        long indexStop = System.currentTimeMillis();
        System.out.println("index flush time: " + (indexStop - indexStart));
        System.out.println("finish insertion");
        System.out.println("finish creation and filling");

        return seriesStore;
    }

    /**
     * the format of record in the file is (time_min, time_max, lon_min, lon_max, lat_min, lat_max), and the time unit is second
     * @param queryFile
     * @return
     */
    public static List<SpatialTemporalRangeQueryPredicate> getSpatialTemporalRangeQueriesFromQueryFile(String queryFile) {
        List<SpatialTemporalRangeQueryPredicate> predicateList = new ArrayList<>();

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile), 1024 * 1024 * 4);
            String currentLine;

            while ((currentLine = bufferedReader.readLine()) != null) {
                String[] items = currentLine.split(",");
                //long time_min = Long.parseLong(items[0]) * 1000;
                //long time_max = Long.parseLong(items[1]) * 1000;
                long time_min = Long.parseLong(items[0]);
                long time_max = Long.parseLong(items[1]);
                double lon_min = Double.parseDouble(items[2]);
                double lon_max = Double.parseDouble(items[3]);
                double lat_min = Double.parseDouble(items[4]);
                double lat_max = Double.parseDouble(items[5]);
                SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(time_min, time_max, new Point(lon_min, lat_min), new Point(lon_max, lat_max));
                predicateList.add(predicate);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return predicateList;
    }

}
