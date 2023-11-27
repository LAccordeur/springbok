package com.rogerguo.test.benchmark.rtree;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.test.BenchmarkRtree;
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
 * @create 2021-12-08 10:56 AM
 **/
public class RStartreeBenchmarkWithGaussian {

    @State(Scope.Benchmark)
    public static class RtreeBenchmarkState {

        @Param({"1000", "10000", "100000", "1000000"})
        public int listSize;

        public List<TrajectoryPoint> pointList = new ArrayList<>();

        @Setup(Level.Trial)
        public void setUp() {
           pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 10, 10);
        }

    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionDefault(Blackhole blackhole, BenchmarkRtree.RtreeBenchmarkState benchmarkState) {
        RTree<String, Geometry> rtree = RTree.star().create();
        for (Point point : benchmarkState.pointList) {
            rtree = rtree.add("test", Geometries.point(point.getLongitude(), point.getLatitude()));
        }
        blackhole.consume(rtree);
    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionChildren32(Blackhole blackhole, BenchmarkRtree.RtreeBenchmarkState benchmarkState) {
        RTree<String, Geometry> rtree = RTree.star().maxChildren(32).create();
        for (Point point : benchmarkState.pointList) {
            rtree = rtree.add("test", Geometries.point(point.getLongitude(), point.getLatitude()));
        }
        blackhole.consume(rtree);
    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionChildren128(Blackhole blackhole, BenchmarkRtree.RtreeBenchmarkState benchmarkState) {
        RTree<String, Geometry> rtree = RTree.star().maxChildren(128).create();
        for (Point point : benchmarkState.pointList) {
            rtree = rtree.add("test", Geometries.point(point.getLongitude(), point.getLatitude()));
        }
        blackhole.consume(rtree);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RStartreeBenchmarkWithGaussian.class.getSimpleName())
                .output("r-star-tree-gaussian.log")
                .build();

        new Runner(opt).run();

    }

}
