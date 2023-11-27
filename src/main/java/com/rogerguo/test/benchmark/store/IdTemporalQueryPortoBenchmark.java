package com.rogerguo.test.benchmark.store;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.flush.*;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2022-06-23 9:49 AM
 **/
public class IdTemporalQueryPortoBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        public int objectSize = IngestionForQuery.objectSize;

        public int numOfConnection = IngestionForQuery.numOfConnection;

        @Param({"SPATIO_TEMPORAL", "SPATIO_TEMPORAL_STR"})
        public S3LayoutSchemaName s3LayoutSchemaName;

        public boolean flushToS3 = IngestionForQuery.flushToS3;

        @Param({"3600", "21600", "86400", "604800"})
        public int timeLength;  // unit is s, need to be converted to ms for query predicate

        @Param({"10"})
        public int querySetSize;

        List<IdTemporalQueryPredicate> predicateList;

        SeriesStore seriesStore;

        TieredCloudStorageManager storageManager;

        ToDiskFlushPolicy toDiskFlushPolicy;

        ToS3FlushPolicy toS3FlushPolicy;

        PortoTaxiRealData portoTaxiRealData;

        @Setup(Level.Trial)
        public void setup() {
            String mybucketName = "";
            if (s3LayoutSchemaName == S3LayoutSchemaName.SPATIO_TEMPORAL) {
                mybucketName = "flush-test-1111-st";
            } else if (s3LayoutSchemaName == S3LayoutSchemaName.SPATIO_TEMPORAL_STR) {
                mybucketName = "flush-test-1111-str";
            }

            // dimension normalizer
            DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

            // parameters for in-memory chunk size (the max number of points in a chunk)
            int maxChunkSize = 200;

            // parameters for the index for immutable chunks
            int indexNodeSize = 1024;
            Region region = Region.AP_EAST_1;
            String bucketName = mybucketName;
            String rootDirnameInBucket = "index-test";
            boolean lazyParentUpdate = true;
            boolean preciseSpatialIndex = true;
            boolean enableSpatialIndex = true;
            IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
            SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
            indexForImmutable = indexForImmutable.loadAndRebuildIndex();

            // parameters for the index for head chunks
            int geoHashShiftLength = 12;
            int postingListCapacity = 50;
            HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

            // parameters for tiered storage
            String bucketNameFrStorage = mybucketName;
            Region regionForStorage = Region.AP_EAST_1;

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
            DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
            ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

            storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

            // series store
            seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

            List<Integer> randomIndexList = new ArrayList<>();
            Random random = new Random(1);
            for (int i = 0; i < querySetSize; i++) {
                randomIndexList.add(random.nextInt(10000000));
            }

            portoTaxiRealData = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");
            predicateList = new ArrayList<>();
            TrajectoryPoint point = null;
            int count = 0;
            while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
                count++;
                if (count % 1000000 == 0) {
                    System.out.println(count);
                }
                if (randomIndexList.contains(count)) {
                    IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(point.getTimestamp(), point.getTimestamp()+timeLength * 1000L, point.getOid());
                    predicateList.add(predicate);
                }

            }
            System.out.println("bucket name: " + bucketName);
            System.out.println("begin query benchmark");
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String statusFilename = "/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/storage/status/idtemporal-status.log";

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

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(10)
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void runQuery(Blackhole blackhole, BenchmarkState state) {
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            List<TrajectoryPoint> result = state.seriesStore.idTemporalQueryWithRefinement(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());
            blackhole.consume(result);
            System.out.println("object store request count: " + ObjectStoreDriver.getCount);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IdTemporalQueryPortoBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
