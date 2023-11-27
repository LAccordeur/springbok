package com.rogerguo.test.benchmark.headchunk;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2021-12-08 11:45 AM
 **/
public class DefaultQueryBenchmark {

    @State(Scope.Benchmark)
    public static class QueryBenchmarkState {

        @Param({"1000"})
        public int querySetSize;

        @Param({"1000000"})
        public long listSize;

        public SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();

        public List<SpatialBoundingBox> queryPredicateList;

        public HeadChunkIndexWithRtree headChunkIndexWithRtree;

        public HeadChunkIndexWithRtreeMBR headChunkIndexWithRtreeMBR;

        public HeadChunkIndexWithGeoHash headChunkIndexWithGeoHash;

        public HeadChunkIndexWithGeoHashSemiSplit headChunkIndexWithGeoHashSemiSplit;

        public HeadChunkIndexWithGeoHashPhysicalSplit headChunkIndexWithGeoHashPhysicalSplit;

        @Setup(Level.Trial)
        public void setup() {
            System.out.println("setup state");
            seriesStore = SeriesStore.initNewStoreForInMemTest();
            queryPredicateList = new ArrayList<>();
            headChunkIndexWithRtree = new HeadChunkIndexWithRtree();
            headChunkIndexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR();
            headChunkIndexWithGeoHash = new HeadChunkIndexWithGeoHash();
            headChunkIndexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit();
            headChunkIndexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(seriesStore);

            Random random = new Random();
            for (long i = 0; i < listSize; i++) {
                TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), i, random.nextDouble()*180, random.nextDouble()*90);
                seriesStore.appendSeriesPoint(point);
                headChunkIndexWithRtree.updateIndex(point);
                headChunkIndexWithRtreeMBR.updateIndex(point);
                headChunkIndexWithGeoHash.updateIndex(point);
                headChunkIndexWithGeoHashSemiSplit.updateIndex(point);
                headChunkIndexWithGeoHashPhysicalSplit.updateIndex(point);
            }

            for (int i = 0; i < querySetSize; i++) {
                double xLow = random.nextDouble()*180;
                double xHigh = xLow + 0.1;
                double yLow = random.nextDouble()*90;
                double yHigh = yLow + 0.1;
                queryPredicateList.add(new SpatialBoundingBox(new Point(xLow, yLow), new Point(xHigh, yHigh)));
            }

        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(1000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void GeoHashQuery(Blackhole blackhole, QueryBenchmarkState state) {
        for (SpatialBoundingBox spatialBoundingBox : state.queryPredicateList) {
            Set<String> result = state.headChunkIndexWithGeoHash.searchForSpatial(spatialBoundingBox);
            blackhole.consume(result);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(1000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void GeoHashSemiSplitQuery(Blackhole blackhole, QueryBenchmarkState state) {
        for (SpatialBoundingBox spatialBoundingBox : state.queryPredicateList) {
            Set<String> result = state.headChunkIndexWithGeoHashSemiSplit.searchForSpatial(spatialBoundingBox);
            blackhole.consume(result);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(1000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void GeoHashPhysicalSplitQuery(Blackhole blackhole, QueryBenchmarkState state) {
        for (SpatialBoundingBox spatialBoundingBox : state.queryPredicateList) {
            Set<String> result = state.headChunkIndexWithGeoHashPhysicalSplit.searchForSpatial(spatialBoundingBox);
            blackhole.consume(result);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(1000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void RtreeQuery(Blackhole blackhole, QueryBenchmarkState state) {
        for (SpatialBoundingBox spatialBoundingBox : state.queryPredicateList) {
            Set<String> result = state.headChunkIndexWithRtree.searchForSpatial(spatialBoundingBox);
            blackhole.consume(result);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(1000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void RtreeMBRQuery(Blackhole blackhole, QueryBenchmarkState state) {
        for (SpatialBoundingBox spatialBoundingBox : state.queryPredicateList) {
            Set<String> result = state.headChunkIndexWithRtreeMBR.searchForSpatial(spatialBoundingBox);
            blackhole.consume(result);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DefaultQueryBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();

    }
}
