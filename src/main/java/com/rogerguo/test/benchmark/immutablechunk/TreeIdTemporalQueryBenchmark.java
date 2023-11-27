package com.rogerguo.test.benchmark.immutablechunk;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.index.NodeTuple;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;
import com.rogerguo.test.index.util.IndexConfiguration;
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
 * @Description
 *
 * evaluation id temporal queries (vary the length of time range)
 *
 * compare to variations with different block size
 *
 * @Date 2021/12/25 15:35
 * @Created by X1 Carbon
 */
public class TreeIdTemporalQueryBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {

        @Param({"1000000"})
        public int listSize;

        @Param({"500"})
        public int querySetSize;

        @Param({"256", "512", "1024"})
        public int nodeSize;

        @Param({"10", "100", "1000"})
        public int timeRange;

        public List<IdTemporalQueryPredicate> queryPredicateList;

        List<TrajectorySegmentMeta> metaList;

        public SpatialTemporalTree indexTree;

        @Setup(Level.Trial)
        public void setup() {
            queryPredicateList = new ArrayList<>();
            Region region = Region.AP_EAST_1;
            String bucketName = "bucket-for-index-20101010";
            String rootDirnameInBucket = "index-test";
            IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, true, bucketName, rootDirnameInBucket, region, true, true);
            indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

            metaList = SyntheticDataGenerator.generateRandomDistributedIndexEntries(listSize, 1, 1);
            for (TrajectorySegmentMeta segmentMeta : metaList) {
                indexTree.insert(segmentMeta);
            }

            Random random = new Random(3);
            for (int i = 0; i < querySetSize; i++) {
                int randomValue = Math.abs(random.nextInt(listSize));
                IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(randomValue, randomValue + timeRange, String.valueOf(randomValue));
                queryPredicateList.add(predicate);
            }

        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexTree.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/immutablechunk/status/tree-idtemporal-query.log", status);
            System.out.println(status);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {
        @Param("1000000")
        public int listSize;

        @Param({"500"})
        public int querySetSize;

        @Param({"256", "512", "1024"})
        public int nodeSize;

        @Param({"10", "100", "1000"})
        public int timeRange;

        public List<IdTemporalQueryPredicate> queryPredicateList;

        List<TrajectorySegmentMeta> metaList;

        public SpatialTemporalTree indexTree;

        @Setup(Level.Trial)
        public void setup() {
            queryPredicateList = new ArrayList<>();
            Region region = Region.AP_EAST_1;
            String bucketName = "bucket-for-index-20101010";
            String rootDirnameInBucket = "index-test";
            IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, true, bucketName, rootDirnameInBucket, region, true, true);
            indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

            metaList = SyntheticDataGenerator.generateGaussianDistributedIndexEntries(listSize, 1, 1);
            for (TrajectorySegmentMeta segmentMeta : metaList) {
                indexTree.insert(segmentMeta);
            }

            Random random = new Random(3);
            for (int i = 0; i < querySetSize; i++) {
                int randomIndex = Math.abs(random.nextInt(listSize));
                TrajectorySegmentMeta meta = metaList.get(randomIndex);
                IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(meta.getStartTimestamp(), meta.getStartTimestamp() + timeRange, String.valueOf(meta.getDeviceId()));
                queryPredicateList.add(predicate);
            }

        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexTree.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/immutablechunk/status/tree-idtemporal-gaussian.log", status);
            System.out.println(status);
        }
    }


    @Fork(value = 1)
    @Warmup(iterations = 2, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void queryRandom(Blackhole blackhole, BenchmarkStateRandom stateRandom) {
        for (IdTemporalQueryPredicate predicate : stateRandom.queryPredicateList) {
            List<NodeTuple> tuples = stateRandom.indexTree.searchForIdTemporal(predicate);
            RefineUtilForTest.refineIdTemporal(predicate, tuples, stateRandom.metaList);
            blackhole.consume(tuples);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 2, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void queryGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (IdTemporalQueryPredicate predicate : stateGaussian.queryPredicateList) {
            List<NodeTuple> tuples = stateGaussian.indexTree.searchForIdTemporal(predicate);
            RefineUtilForTest.refineIdTemporal(predicate, tuples, stateGaussian.metaList);
            blackhole.consume(tuples);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TreeIdTemporalQueryBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/immutablechunk/tree-idtemporal-test.log")
                .build();

        new Runner(opt).run();
    }

}
