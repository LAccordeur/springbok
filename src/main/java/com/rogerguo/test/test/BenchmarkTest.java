package com.rogerguo.test.test;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author yangguo
 * @create 2021-12-08 10:30 AM
 **/
public class BenchmarkTest {

    @State(Scope.Benchmark)
    public static class MyBenchmarkState {
        int count = 0;
    }

    @State(Scope.Thread)
    public static class MyBenchmarkStateUnshared {
        int count = 0;
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @Measurement(time = 1, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void shared(MyBenchmarkState myBenchmarkState) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(5000);
        myBenchmarkState.count++;
        System.out.println(myBenchmarkState.count);
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @Measurement(time = 1, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void unshared(MyBenchmarkStateUnshared myBenchmarkStateUnshared) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(5000);
        myBenchmarkStateUnshared.count++;
        System.out.println(myBenchmarkStateUnshared.count);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
