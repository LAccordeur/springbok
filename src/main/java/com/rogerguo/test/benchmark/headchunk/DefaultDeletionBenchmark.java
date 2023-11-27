package com.rogerguo.test.benchmark.headchunk;

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
 * @create 2021-12-08 12:54 PM
 **/
public class DefaultDeletionBenchmark {

    @State(Scope.Benchmark)
    public static class DeletionBenchmarkState {

        @Param({"1000000"})
        public long listSize;

        @Param({"10000","100000"})
        public int deletionSetSize;

        public List<Chunk> deletionList;

        public SeriesStore seriesStore;

        public HeadChunkIndexWithRtree headChunkIndexWithRtree;

        public HeadChunkIndexWithRtreeMBR headChunkIndexWithRtreeMBR;

        public HeadChunkIndexWithGeoHash headChunkIndexWithGeoHash;

        public HeadChunkIndexWithGeoHashSemiSplit headChunkIndexWithGeoHashSemiSplit;

        public HeadChunkIndexWithGeoHashPhysicalSplit headChunkIndexWithGeoHashPhysicalSplit;

        @Setup(Level.Invocation)
        public void setup() {
            System.out.println("setup state");
            deletionList = new ArrayList<>();
            seriesStore = SeriesStore.initNewStoreForInMemTest();
            headChunkIndexWithRtree = new HeadChunkIndexWithRtree();
            headChunkIndexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR();
            headChunkIndexWithGeoHash = new HeadChunkIndexWithGeoHash();
            headChunkIndexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit();
            headChunkIndexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(seriesStore);


            Random random = new Random();
            for (long i = 0; i < listSize; i++) {
                TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), 1, random.nextDouble()*180, random.nextDouble()*90);
                seriesStore.appendSeriesPoint(point);
                headChunkIndexWithRtree.updateIndex(point);
                headChunkIndexWithRtreeMBR.updateIndex(point);
                headChunkIndexWithGeoHash.updateIndex(point);
                headChunkIndexWithGeoHashSemiSplit.updateIndex(point);
                headChunkIndexWithGeoHashPhysicalSplit.updateIndex(point);

                if (deletionList.size() < 100000) {
                    Chunk chunk = new Chunk(String.valueOf(i));
                    List<TrajectoryPoint> pointList = new ArrayList<>();
                    pointList.add(point);
                    chunk.setChunk(pointList);
                    deletionList.add(chunk);
                }
            }

        }
    }

    /*@Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(100000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void baseline() {
        // do nothing
    }*/

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(100000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void geoHashDeletion(Blackhole blackhole, DeletionBenchmarkState state) {
        for (Chunk chunk : state.deletionList) {
            state.headChunkIndexWithGeoHash.removeFromIndex(chunk);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(100000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void geoHashPhysicalSplitDeletion(Blackhole blackhole, DeletionBenchmarkState state) {
        for (Chunk chunk : state.deletionList) {
            state.headChunkIndexWithGeoHashPhysicalSplit.removeFromIndex(chunk);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(100000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void geoHashSemiSplitDeletion(Blackhole blackhole, DeletionBenchmarkState state) {
        for (Chunk chunk : state.deletionList) {
            state.headChunkIndexWithGeoHashSemiSplit.removeFromIndex(chunk);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(100000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void rtreeDeletion(Blackhole blackhole, DeletionBenchmarkState state) {
        for (Chunk chunk : state.deletionList) {
            state.headChunkIndexWithRtree.removeFromIndex(chunk);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(100000)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void rtreembrDeletion(Blackhole blackhole, DeletionBenchmarkState state) {
        for (Chunk chunk : state.deletionList) {
            state.headChunkIndexWithRtreeMBR.removeFromIndex(chunk);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DefaultDeletionBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
