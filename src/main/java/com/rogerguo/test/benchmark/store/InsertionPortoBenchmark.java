package com.rogerguo.test.benchmark.store;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.recovery.LeafNodeStatusRecorder;
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
 * @create 2022-06-23 10:12 AM
 **/
public class InsertionPortoBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        public int objectSize = 2000;

        public int numOfConnection = 1;

        //@Param({"SPATIO_TEMPORAL", "SPATIO_TEMPORAL_STR", "SINGLE_TRAJECTORY"})
        @Param({"SPATIO_TEMPORAL_STR"})
        public S3LayoutSchemaName s3LayoutSchemaName;

        @Param({"false"})
        public boolean flushToS3;

        @Param({"true"})
        public boolean enableRecovery;

        //@Param({"object", "disk"})
        @Param({"disk"})
        public String recoveryMode;

        public SeriesStore seriesStore;

        TieredCloudStorageManager storageManager;

        ToDiskFlushPolicy toDiskFlushPolicy;

        ToS3FlushPolicy toS3FlushPolicy;

        PortoTaxiRealData portoTaxiRealData;

        LeafNodeStatusRecorder leafNodeStatusRecorder;

        ObjectStoreDriver objectStoreDriver = new ObjectStoreDriver("flush-test-1111-from-data", Region.AP_EAST_1, "index-test-recovery");

        DiskDriver diskDriver = new DiskDriver("/home/yangguo/IdeaProjects/trajectory-index/recovery-test");

        @Setup(Level.Invocation)
        public void setupIndex() {

            //boolean flushToS3 = true;

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
            IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex, enableRecovery);

            System.out.println(enableRecovery);
            System.out.println(recoveryMode);
            // set for recover
            if (enableRecovery) {

                if ("object".equals(recoveryMode)) {
                    System.out.println("init object index recorder");
                    leafNodeStatusRecorder = new LeafNodeStatusRecorder(objectStoreDriver);
                } else {
                    System.out.println("init disk index recorder");
                    leafNodeStatusRecorder = new LeafNodeStatusRecorder(diskDriver);
                }
            }

            SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
            if (enableRecovery) {
                indexForImmutable.setLeafNodeStatusRecorder(leafNodeStatusRecorder);
            }


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
            toDiskFlushPolicy = new ToDiskFlushPolicy();
            if (enableRecovery && "disk".equals(recoveryMode)) {
                toDiskFlushPolicy.setLeafNodeStatusRecorder(leafNodeStatusRecorder);
            }

            toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, 2000, 1);
            if (enableRecovery && "object".equals(recoveryMode)) {
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
            DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/Data/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
            ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

            storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

            // series store
            seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

            portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");

            System.out.println("finish");
        }
    }


    @Fork(1)
    @Warmup(iterations = 0)
    @Benchmark
    @Measurement(time = 20, iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insert(Blackhole blackhole, BenchmarkState state) {
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
        //System.out.println(leafNodeStatusRecorder.getFullLeafNodeList().get(0).getNotFlushedBlockIds());
        if (state.leafNodeStatusRecorder != null) {
            state.leafNodeStatusRecorder.printStatus();
        }
        blackhole.consume(state.seriesStore);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(InsertionPortoBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
