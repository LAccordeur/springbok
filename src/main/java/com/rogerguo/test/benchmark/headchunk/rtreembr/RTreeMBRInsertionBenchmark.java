package com.rogerguo.test.benchmark.headchunk.rtreembr;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.HeadChunkIndexWithRtreeMBR;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2021-12-24 12:51 PM
 **/
public class RTreeMBRInsertionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {
        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"4", "8", "16"})
        public int nodeSize;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithRtreeMBR indexWithRtreeMBR;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1 ,1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            indexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR(nodeSize);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithRtreeMBR.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/rtree-mbr-insertion-random.log", status);

        }
    }

    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 5, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionRandom(Blackhole blackhole, BenchmarkStateRandom stateRandom) {
        for (TrajectoryPoint point : stateRandom.pointList) {
            stateRandom.indexWithRtreeMBR.updateIndex(point);
        }
        blackhole.consume(stateRandom.indexWithRtreeMBR);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {
        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"4", "8", "16"})
        public int nodeSize;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithRtreeMBR indexWithRtreeMBR;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1 ,1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            indexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR(nodeSize);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithRtreeMBR.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/rtree-mbr-insertion-gaussian.log", status);

        }
    }

    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 5, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (TrajectoryPoint point : stateGaussian.pointList) {
            stateGaussian.indexWithRtreeMBR.updateIndex(point);
        }
        blackhole.consume(stateGaussian.indexWithRtreeMBR);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RTreeMBRInsertionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/rtreembr/rtreembr-insertion.log")
                .build();
        new Runner(opt).run();
    }
}
