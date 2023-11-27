package com.rogerguo.test.benchmark.headchunk.physical;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashPhysicalSplit;
import com.rogerguo.test.store.SeriesStore;
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
 * @create 2021-12-22 5:05 PM
 **/
public class PhyInsertionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {

        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"8", "12", "16"})
        public int shiftLength;

        @Param({"50", "100", "200"})
        public int postingListCapacity;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit;

        SeriesStore seriesStore;

        @Setup(Level.Trial)
        public void setupData() {
            seriesStore = SeriesStore.initNewStoreForInMemTest();
            pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1, 1);
            for (TrajectoryPoint point : pointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }

        @Setup(Level.Invocation)
        public void setupIndex() {

            indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(shiftLength, postingListCapacity, seriesStore);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHashPhysicalSplit.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-phy-insertion-random.log", status);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {

        @Param({"100000", "1000000", "10000000"})
        public long listSize;

        @Param({"8", "12", "16"})
        public int shiftLength;

        @Param({"50", "100", "200"})
        public int postingListCapacity;

        public List<TrajectoryPoint> pointList;

        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit;

        SeriesStore seriesStore;

        @Setup(Level.Trial)
        public void setupData() {
            seriesStore = SeriesStore.initNewStoreForInMemTest();
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);
            for (TrajectoryPoint point : pointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }

        @Setup(Level.Invocation)
        public void setupIndex() {

            indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(shiftLength, postingListCapacity, seriesStore);
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHashPhysicalSplit.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-phy-insertion-gaussian.log", status);
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
            stateRandom.indexWithGeoHashPhysicalSplit.updateIndex(point);
        }

        blackhole.consume(stateRandom.indexWithGeoHashPhysicalSplit);
    }

    @Fork(1)
    @Warmup(iterations = 2)
    @Benchmark
    @Measurement(time = 5, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (TrajectoryPoint point : stateGaussian.pointList) {
            stateGaussian.indexWithGeoHashPhysicalSplit.updateIndex(point);
        }

        blackhole.consume(stateGaussian.indexWithGeoHashPhysicalSplit);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PhyInsertionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/phy/phy-insertion.log")
                .build();

        new Runner(opt).run();
    }
}
