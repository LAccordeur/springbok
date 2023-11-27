package com.rogerguo.test.benchmark.immutablechunk;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.TrajectorySegmentMeta;
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
import java.util.concurrent.TimeUnit;

/**
 * vary data size: use different dataset / data size to evaluate them
 *
 * compare to several variation with/without activeNode
 *
 * @Description
 * @Date 2021/12/25 15:12
 * @Created by yangguo
 */
public class TreeInsertionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {

        @Param({"10000","100000","1000000"})
        public long listSize;

        @Param({"256", "512", "1024"})
        public int nodeSize;  // the max entries in a node

        @Param({"true", "false"})
        public boolean lazyUpdate;

        @Param({"true", "false"})
        public boolean preciseSpatialIndex;

        public List<TrajectorySegmentMeta> segmentMetaList;

        public SpatialTemporalTree indexTree;

        @Setup(Level.Trial)
        public void setupData() {
            segmentMetaList = SyntheticDataGenerator.generateRandomDistributedIndexEntries(listSize, 1, 1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            Region region = Region.AP_EAST_1;
            String bucketName = "bucket-for-index-20101010";
            String rootDirnameInBucket = "index-test";
            IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, lazyUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, true);
            indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexTree.printStatus();
            System.out.println(status);
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/immutablechunk/status/tree-insertion-random.log", status);
        }

    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {
        @Param({"10000","100000","1000000"})
        public long listSize;

        @Param({"256", "512", "1024"})
        public int nodeSize;  // the max entries in a node

        @Param({"true", "false"})
        public boolean lazyUpdate;

        @Param({"true", "false"})
        public boolean preciseSpatialIndex;

        public List<TrajectorySegmentMeta> segmentMetaList;

        public SpatialTemporalTree indexTree;

        @Setup(Level.Trial)
        public void setupData() {
            segmentMetaList = SyntheticDataGenerator.generateGaussianDistributedIndexEntries(listSize, 1, 1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            Region region = Region.AP_EAST_1;
            String bucketName = "bucket-for-index-20101010";
            String rootDirnameInBucket = "index-test";
            IndexConfiguration indexConfiguration = new IndexConfiguration(nodeSize, lazyUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, true);
            indexTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexTree.printStatus();
            System.out.println(status);
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/immutablechunk/status/tree-insertion-gaussian.log", status);
        }

    }

    /**
     * baseline (only for insertion):insertionWithoutActiveNodeTODO
     */


    /**
     * with active node but use real-time parent update
     */
    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionRandom(Blackhole blackhole, BenchmarkStateRandom stateRandom) {
        int count = 0;
        for (TrajectorySegmentMeta meta : stateRandom.segmentMetaList) {
            //count++;
            stateRandom.indexTree.insert(meta);
            //System.out.println(count);
        }
        blackhole.consume(stateRandom.indexTree);
    }

    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (TrajectorySegmentMeta meta : stateGaussian.segmentMetaList) {
            stateGaussian.indexTree.insert(meta);
        }
        blackhole.consume(stateGaussian.indexTree);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TreeInsertionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/immutablechunk/tree-insertion-test.log")
                .build();
        new Runner(opt).run();

    }


}
