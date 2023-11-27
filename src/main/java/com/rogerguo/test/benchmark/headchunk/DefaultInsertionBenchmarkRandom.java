package com.rogerguo.test.benchmark.headchunk;

import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2021-12-08 10:57 AM
 **/
public class DefaultInsertionBenchmarkRandom {

    @State(Scope.Benchmark)
    public static class BenchmarkDataState {

        @Param({"10000", "100000", "1000000"})
        public long listSize;

        public List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 10, 10);

        @Setup(Level.Trial)
        public void setUp() {
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 10, 10);
        }

    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void GeoHashInsertion(Blackhole blackhole, BenchmarkDataState dataState) {
        //System.out.println(dataState.pointList.size());
        HeadChunkIndexWithGeoHash indexWithGeoHash = new HeadChunkIndexWithGeoHash();
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithGeoHash.updateIndex(point);
        }
        blackhole.consume(indexWithGeoHash);
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void GeoHashSemiSplitInsertion(Blackhole blackhole, BenchmarkDataState dataState) {
        HeadChunkIndexWithGeoHashSemiSplit indexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit();
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithGeoHashSemiSplit.updateIndex(point);
        }
        blackhole.consume(indexWithGeoHashSemiSplit);
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void GeoHashPhysicalSplitInsertion(Blackhole blackhole, BenchmarkDataState dataState) {
        SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();
        HeadChunkIndexWithGeoHashPhysicalSplit indexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(seriesStore);
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithGeoHashPhysicalSplit.updateIndex(point);
        }
        blackhole.consume(indexWithGeoHashPhysicalSplit);
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void RtreeInsertion(Blackhole blackhole, BenchmarkDataState dataState) {
        HeadChunkIndexWithRtree indexWithRtree = new HeadChunkIndexWithRtree();
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithRtree.updateIndex(point);
        }
        blackhole.consume(indexWithRtree);
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void RtreeInsertionMBR(Blackhole blackhole, BenchmarkDataState dataState) {
        HeadChunkIndexWithRtreeMBR indexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR();
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithRtreeMBR.updateIndex(point);
        }
        blackhole.consume(indexWithRtreeMBR);
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void RStartreeInsertionMBR(Blackhole blackhole, BenchmarkDataState dataState) {
        HeadChunkIndexWithRStartreeMBR indexWithRStartreeMBR = new HeadChunkIndexWithRStartreeMBR();
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithRStartreeMBR.updateIndex(point);
        }
        blackhole.consume(indexWithRStartreeMBR);
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void RStartreeInsertion(Blackhole blackhole, BenchmarkDataState dataState) {
        HeadChunkIndexWithRStartree indexWithRStartree = new HeadChunkIndexWithRStartree();
        for (TrajectoryPoint point : dataState.pointList) {
            indexWithRStartree.updateIndex(point);
        }
        blackhole.consume(indexWithRStartree);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DefaultInsertionBenchmarkRandom.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
