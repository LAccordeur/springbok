package com.rogerguo.test.benchmark.headchunk.physical;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.HeadChunkIndexWithGeoHashPhysicalSplit;
import com.rogerguo.test.store.HeadChunkIndexWithRStartreeMBR;
import com.rogerguo.test.store.SeriesStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2021-12-24 3:25 PM
 **/
public class PhyDeletionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {

        @Param({"10000000"})
        public long listSize;

        @Param({"1000000"})
        public int deletionSetSize;

        @Param({"8", "12", "16"})
        public int shiftLength;

        @Param({"50", "100"})
        public int postingListCapacity;

        public List<Chunk> deletionList;

        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit;

        public List<TrajectoryPoint> pointList;

        SeriesStore seriesStore;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1, 1);
            seriesStore = SeriesStore.initNewStoreForInMemTest();
            for (TrajectoryPoint point : pointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            deletionList = new ArrayList<>();
            indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(shiftLength, postingListCapacity, seriesStore);

            for (int i = 0; i < pointList.size(); i++) {
                TrajectoryPoint point = pointList.get(i);
                indexWithGeoHashPhysicalSplit.updateIndex(point);

                if (deletionList.size() < deletionSetSize) {
                    Chunk chunk = new Chunk(String.valueOf(i));
                    List<TrajectoryPoint> pointList = new ArrayList<>();
                    pointList.add(point);
                    chunk.setChunk(pointList);
                    deletionList.add(chunk);
                }
            }
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHashPhysicalSplit.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-phy-deletion-random.log", status);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {

        @Param({"10000000"})
        public long listSize;

        @Param({"1000000"})
        public int deletionSetSize;

        @Param({"8", "12", "16"})
        public int shiftLength;

        @Param({"50", "100"})
        public int postingListCapacity;

        public List<Chunk> deletionList;

        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit;

        SeriesStore seriesStore;

        public List<TrajectoryPoint> pointList;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);
            seriesStore = SeriesStore.initNewStoreForInMemTest();
            for (TrajectoryPoint point : pointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            deletionList = new ArrayList<>();
            indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(shiftLength, postingListCapacity, seriesStore);

            for (int i = 0; i < pointList.size(); i++) {
                TrajectoryPoint point = pointList.get(i);
                indexWithGeoHashPhysicalSplit.updateIndex(point);

                if (deletionList.size() < deletionSetSize) {
                    Chunk chunk = new Chunk(String.valueOf(i));
                    List<TrajectoryPoint> pointList = new ArrayList<>();
                    pointList.add(point);
                    chunk.setChunk(pointList);
                    deletionList.add(chunk);
                }
            }
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String status = indexWithGeoHashPhysicalSplit.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/geohash-phy-deletion-gaussian.log", status);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 2)
    @Benchmark
    @OperationsPerInvocation(1000000)
    @BenchmarkMode(Mode.AverageTime)
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void deletionRandom(Blackhole blackhole, BenchmarkStateRandom stateRandom) {
        for (Chunk chunk : stateRandom.deletionList) {
            stateRandom.indexWithGeoHashPhysicalSplit.removeFromIndex(chunk);
        }
        blackhole.consume(stateRandom.indexWithGeoHashPhysicalSplit);
    }

    @Fork(value = 1)
    @Warmup(iterations = 2)
    @Benchmark
    @OperationsPerInvocation(1000000)
    @BenchmarkMode(Mode.AverageTime)
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void deletionGaussian(Blackhole blackhole, BenchmarkStateGaussian stateGaussian) {
        for (Chunk chunk : stateGaussian.deletionList) {
            stateGaussian.indexWithGeoHashPhysicalSplit.removeFromIndex(chunk);
        }
        blackhole.consume(stateGaussian.indexWithGeoHashPhysicalSplit);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PhyDeletionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/phy/phy-deletion.log")
                .build();

        new Runner(opt).run();
    }

}
