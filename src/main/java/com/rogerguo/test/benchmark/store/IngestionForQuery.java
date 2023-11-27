package com.rogerguo.test.benchmark.store;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
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
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2022-06-22 3:27 PM
 **/
public class IngestionForQuery {

    public static int objectSize = 2000;

    public static int numOfConnection = 1;

    public static boolean flushToS3 = true;

    public static void ingestData(String dataFile, S3LayoutSchemaName s3LayoutSchemaName, String bucketName) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.AP_EAST_1;
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
        Region regionForStorage = Region.AP_EAST_1;

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
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");



        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFile);

        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
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
        indexForImmutable.serializeAndFlushIndex();
        System.out.println("finish insertion");
    }

    public static void main(String[] args) {
        String dataFile = "/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv";
        //S3LayoutSchemaName s3LayoutSchemaName = S3LayoutSchemaName.SPATIO_TEMPORAL;
        ingestData(dataFile, S3LayoutSchemaName.SPATIO_TEMPORAL, "flush-test-1111-st");
        //ingestData(dataFile, S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-1111-str");
        //ingestData(dataFile, S3LayoutSchemaName.SINGLE_TRAJECTORY);
    }

}
