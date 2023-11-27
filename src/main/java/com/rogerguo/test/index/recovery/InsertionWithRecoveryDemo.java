package com.rogerguo.test.index.recovery;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.storage.flush.ToDiskFlushPolicy;
import com.rogerguo.test.storage.flush.ToS3FlushPolicy;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import software.amazon.awssdk.regions.Region;

/**
 * @author yangguo
 * @create 2022-06-22 3:36 PM
 **/
public class InsertionWithRecoveryDemo {

    private static IndexConfiguration indexConfiguration = new IndexConfiguration(1024, true, "springbok-store-csv-test", "index-test-data", Region.US_EAST_1, true, true, true);

    private static ObjectStoreDriver objectStoreDriver = new ObjectStoreDriver("springbok-store-csv-test", Region.US_EAST_1, "index-test-recovery");

    private static DiskDriver diskDriver = new DiskDriver("/home/ubuntu/data/recovery-test");

    public static void insertionWithDiskCheckpoint() {

        LeafNodeStatusRecorder leafNodeStatusRecorder = new LeafNodeStatusRecorder(diskDriver);

        boolean flushToS3 = false;

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);
        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConfiguration);
        indexForImmutable.setLeafNodeStatusRecorder(leafNodeStatusRecorder);


        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        toDiskFlushPolicy.setLeafNodeStatusRecorder(leafNodeStatusRecorder);

        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, 2000, 1);

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
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/Data/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");

        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            seriesStore.appendSeriesPoint(point);

            count++;
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
        }

        if (flushToS3) {
            seriesStore.stop();
        } else {
            seriesStore.flushDataToDisk();
        }
        //System.out.println(leafNodeStatusRecorder.getFullLeafNodeList().get(0).getNotFlushedBlockIds());
        leafNodeStatusRecorder.printStatus();
        System.out.println("finish");
    }

    public static void rebuildFromDiskCheckpoint() {
        SpatialTemporalTree tree = IndexTreeBuilder.rebuildIndexTree(diskDriver, indexConfiguration);
        System.out.println(tree.printStatus());
        System.out.println("finish");
    }

    public static void insertionWithObjectStoreCheckpoint() {
        LeafNodeStatusRecorder leafNodeStatusRecorder = new LeafNodeStatusRecorder(objectStoreDriver);

        boolean flushToS3 = true;
        String mode = "object";

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);
        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConfiguration);
        indexForImmutable.setLeafNodeStatusRecorder(leafNodeStatusRecorder);


        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "springbok-store-csv-test";
        Region regionForStorage = Region.US_EAST_1;
        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        if ("disk".equals(mode)) {
            toDiskFlushPolicy.setLeafNodeStatusRecorder(leafNodeStatusRecorder);
        }

        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, 2000, 1);
        if ("object".equals(mode)) {
            toS3FlushPolicy.setLeafNodeStatusRecorder(leafNodeStatusRecorder);
        }

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

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/ubuntu/data/porto_data_v1.csv");

        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            seriesStore.appendSeriesPoint(point);

            count++;
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
        }

        if (flushToS3) {
            seriesStore.stop();
        } else {
            seriesStore.flushDataToDisk();
        }
        //System.out.println(leafNodeStatusRecorder.getFullLeafNodeList().get(0).getNotFlushedBlockIds());
        leafNodeStatusRecorder.printStatus();
        System.out.println("finish");
    }

    public static void rebuildFromObjectStoreCheckpoint() {
        long start = System.currentTimeMillis();
        SpatialTemporalTree tree = IndexTreeBuilder.rebuildIndexTree(objectStoreDriver, indexConfiguration);
        long stop = System.currentTimeMillis();
        System.out.println("rebuild time:" + (stop - start));
        System.out.println(tree.printStatus());
        System.out.println("finish");
    }

    public static void main(String[] args) {
        //insertionWithObjectStoreCheckpoint();
        rebuildFromObjectStoreCheckpoint();
    }

}
