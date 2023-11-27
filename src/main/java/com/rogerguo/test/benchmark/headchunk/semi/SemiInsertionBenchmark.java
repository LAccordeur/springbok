package com.rogerguo.test.benchmark.headchunk.semi;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashSemiSplit;
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
 * @create 2021-12-21 8:28 PM
 **/
public class SemiInsertionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {
        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"8", "12", "16", "32"})
        public int shiftLength;

        @Param({"50", "100", "200"})
        public int postingListCapacity;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithGeoHashSemiSplit indexWithGeoHashSemiSplit;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1, 1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            indexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit(shiftLength, postingListCapacity);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHashSemiSplit.printStatus() + ", listSize = " + listSize + "\n\n";
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-semi-insertion-random.log", status);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {
        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"8", "12", "16", "32"})
        public int shiftLength;

        @Param({"50", "100", "200"})
        public int postingListCapacity;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithGeoHashSemiSplit indexWithGeoHashSemiSplit;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            indexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit(shiftLength, postingListCapacity);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHashSemiSplit.printStatus() + ", listSize = " + listSize + "\n\n";
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-semi-insertion-gaussian.log", status);
        }
    }

    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionRandom(Blackhole blackhole, BenchmarkStateRandom stateRandom) {
        for (TrajectoryPoint point : stateRandom.pointList) {
            stateRandom.indexWithGeoHashSemiSplit.updateIndex(point);
        }
        blackhole.consume(stateRandom.indexWithGeoHashSemiSplit);
    }

    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (TrajectoryPoint point : stateGaussian.pointList) {
            stateGaussian.indexWithGeoHashSemiSplit.updateIndex(point);
        }
        blackhole.consume(stateGaussian.indexWithGeoHashSemiSplit);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SemiInsertionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/semi/semi-insertion.log")
                .build();

        new Runner(opt).run();
    }

}
