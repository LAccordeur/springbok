package com.rogerguo.test.benchmark.storage;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.*;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.storage.layer.StorageLayer;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.rogerguo.test.store.SeriesStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.awssdk.regions.Region;

import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2022-01-04 5:44 PM
 **/
public class StorageInsertionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"2000"})
        public int objectSize;

        @Param({"1"})
        public int numOfConnection; // more connection can reduce the time (before reaching the bandwidth limit)

        @Param({"SPATIO_TEMPORAL", "SPATIO_TEMPORAL_STR", "SINGLE_TRAJECTORY"})
        public S3LayoutSchemaName s3LayoutSchemaName;

        @Param({"true"})
        public boolean flushToS3;

        public SeriesStore seriesStore;

        TieredCloudStorageManager storageManager;

        ToDiskFlushPolicy toDiskFlushPolicy;

        ToS3FlushPolicy toS3FlushPolicy;

        PortoTaxiRealData portoTaxiRealData;


        @Setup(Level.Invocation)
        public void setupIndex() {
            // dimension normalizer
            DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

            // parameters for in-memory chunk size (the max number of points in a chunk)
            int maxChunkSize = 200;

            // parameters for the index for immutable chunks
            int indexNodeSize = 1024;
            Region region = Region.AP_EAST_1;
            String bucketName = "flush-test-1111";
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
            String bucketNameFrStorage = "flush-test-1111";
            Region regionForStorage = Region.AP_EAST_1;
            //int objectSize = 10000;
            //int numOfConnection = 2;
            int s3TimePartition = 1000 * 60 * 60 * 24;
            int s3SpatialPartition= 24;
            S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
            toDiskFlushPolicy = new ToDiskFlushPolicy();
            toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

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

            storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

            // series store
            seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema);

            portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String statusFilename = "/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/storage/status/insertion-status.log";
            for (StorageLayerName storageLayerName : storageManager.getStorageLayerHierarchyNameList()) {
                StorageLayer storageLayer = storageManager.getStorageLayerMap().get(storageLayerName);
                System.out.println(storageLayer.printStatus());
                StatusRecorder.recordStatus(statusFilename, storageLayer.printStatus());
            }
            System.out.println(toDiskFlushPolicy.printStatus());
            System.out.println(toS3FlushPolicy.printStatus());
            StatusRecorder.recordStatus(statusFilename, toDiskFlushPolicy.printStatus());
            StatusRecorder.recordStatus(statusFilename, toS3FlushPolicy.printStatus());
            if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
                System.out.println(S3SpatioTemporalLayoutSchemaTool.printStatus());
                StatusRecorder.recordStatus(statusFilename, S3SpatioTemporalLayoutSchemaTool.printStatus());
            } else if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
                System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
                StatusRecorder.recordStatus(statusFilename, S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
            } else if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
                System.out.println(S3SingleTrajectoryLayoutSchemaTool.printStatus());
                StatusRecorder.recordStatus(statusFilename, S3SingleTrajectoryLayoutSchemaTool.printStatus());
            }
        }

    }

/*    @Fork(1)
    @Warmup(iterations = 1)
    @Benchmark
    @Measurement(time = 20, iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionRandom(Blackhole blackhole, BenchmarkState state) {
        TrajectoryPoint point = null;
        int count = 0;
        while (true) {
            count++;
            point = SyntheticDataGenerator.nextRandomTrajectoryPoint();
            state.seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
            if (count >= 1000000000) {
                break;
            }
        }
        state.seriesStore.flushDataToDisk();
        blackhole.consume(state.seriesStore);
    }*/

    @Fork(1)
    @Warmup(iterations = 0)
    @Benchmark
    @Measurement(time = 20, iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionPortoTaxi(Blackhole blackhole, BenchmarkState state) {
        TrajectoryPoint point = null;
        int count = 0;
        while ((point = state.portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            state.seriesStore.appendSeriesPoint(point);

            count++;
            if (count % 1000000 == 0) {
                System.out.println(count);
            }
        }

        if (state.flushToS3) {
            state.seriesStore.stop();
        } else {
            state.seriesStore.flushDataToDisk();
        }

        //state.seriesStore.stop();
        blackhole.consume(state.seriesStore);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StorageInsertionBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

}
