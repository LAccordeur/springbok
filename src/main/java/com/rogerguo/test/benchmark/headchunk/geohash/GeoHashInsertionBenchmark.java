package com.rogerguo.test.benchmark.headchunk.geohash;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHash;
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
 * @create 2021-12-24 2:42 PM
 **/
public class GeoHashInsertionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {
        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"8", "12", "16"})
        public int shiftLength;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithGeoHash indexWithGeoHash;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1 ,1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            indexWithGeoHash = new HeadChunkIndexWithGeoHash(shiftLength);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHash.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-insertion-random.log", status);
        }

    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {
        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"8", "12", "16"})
        public int shiftLength;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithGeoHash indexWithGeoHash;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1 ,1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            indexWithGeoHash = new HeadChunkIndexWithGeoHash(shiftLength);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHash.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-insertion-gaussian.log", status);
        }

    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionRandom(Blackhole blackhole, BenchmarkStateRandom stateRandom) {
        for (TrajectoryPoint point : stateRandom.pointList) {
            stateRandom.indexWithGeoHash.updateIndex(point);
        }
        blackhole.consume(stateRandom.indexWithGeoHash);
    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (TrajectoryPoint point : stateGaussian.pointList) {
            stateGaussian.indexWithGeoHash.updateIndex(point);
        }
        blackhole.consume(stateGaussian.indexWithGeoHash);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GeoHashInsertionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/geohash/geohash-insertion.log")
                .build();
        new Runner(opt).run();
    }
}
