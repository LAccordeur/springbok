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
 * @create 2021-12-07 9:20 PM
 **/
public class BenchmarkRtreeSingle {

    @State(Scope.Benchmark)
    public static class RtreeBenchmarkState {

        @Param({"1000", "10000"})
        public int listSize;

        public List<Point> pointList = new ArrayList<>();

        RTree<String, Geometry> rtree = RTree.star().create();

        @Setup(Level.Trial)
        public void setUp() {
            System.out.println("setup data for each trial");
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i < listSize; i++) {
                pointList.add(new Point(random.nextInt(1000), random.nextInt(2000)));
            }
        }

        @Setup(Level.Invocation)
        public void setupTree() {
            System.out.println("setup tree for each invocation");
        }
        /*Random random = new Random(System.currentTimeMillis());

        Point generateNextPoint() {
            return new Point(random.nextInt(1000), random.nextInt(2000));
        }*/

    }

    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(time = 1, iterations = 3)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insertion(Blackhole blackhole, RtreeBenchmarkState benchmarkState) throws InterruptedException {
        Thread.sleep(500);
        for (Point point : benchmarkState.pointList) {
            benchmarkState.rtree = benchmarkState.rtree.add("test", Geometries.point(point.getLongitude(), point.getLatitude()));
            //System.out.println(benchmarkState.rtree.size());
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkRtreeSingle.class.getSimpleName())
                .build();

        new Runner(opt).run();

    }

}
