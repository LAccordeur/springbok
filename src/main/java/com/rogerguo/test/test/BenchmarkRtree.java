package com.rogerguo.test.test;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.rogerguo.test.common.Point;
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
 * @create 2021-12-07 8:33 PM
 **/

public class BenchmarkRtree {

    @State(Scope.Benchmark)
    public static class RtreeBenchmarkState {

        @Param({"1000", "10000", "100000"})
        public int listSize;

        public List<Point> pointList = new ArrayList<>();

        @Setup(Level.Trial)
        public void setUp() {
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i < listSize; i++) {
                pointList.add(new Point(random.nextInt(1000), random.nextInt(2000)));
            }
        }

    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertionDefault(Blackhole blackhole, RtreeBenchmarkState benchmarkState) {
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
    public void insertionChildren32(Blackhole blackhole, RtreeBenchmarkState benchmarkState) {
        RTree<String, Geometry> rtree = RTree.star().maxChildren(32).create();
        for (Point point : benchmarkState.pointList) {
            rtree = rtree.add("test", Geometries.point(point.getLongitude(), point.getLatitude()));
        }
        blackhole.consume(rtree);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkRtree.class.getSimpleName())
                .build();

        new Runner(opt).run();

    }

}
