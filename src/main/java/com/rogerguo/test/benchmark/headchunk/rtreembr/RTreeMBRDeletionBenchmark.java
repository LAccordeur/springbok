package com.rogerguo.test.benchmark.headchunk.rtreembr;

import com.rogerguo.test.benchmark.StatusRecorder;
import com.rogerguo.test.benchmark.SyntheticDataGenerator;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.HeadChunkIndexWithRtreeMBR;
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
 * @create 2021-12-24 12:51 PM
 **/
public class RTreeMBRDeletionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkStateRandom {
        @Param({"10000000"})
        public long listSize;

        @Param({"1000000"})
        public int deletionSetSize;

        @Param({"4", "8", "16"})
        public int nodeSize;

        public List<Chunk> deletionList;

        public HeadChunkIndexWithRtreeMBR indexWithRtreeMBR;

        public List<TrajectoryPoint> pointList;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateRandomDistributedDataset(listSize, 1, 1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            deletionList = new ArrayList<>();
            indexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR(nodeSize);

            for (int i = 0; i < pointList.size(); i++) {
                TrajectoryPoint point = pointList.get(i);
                indexWithRtreeMBR.updateIndex(point);

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
            String status = indexWithRtreeMBR.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/rtree-mbr-deletion-random.log", status);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateGaussian {
        @Param({"10000000"})
        public long listSize;

        @Param({"1000000"})
        public int deletionSetSize;

        @Param({"4", "8", "16"})
        public int nodeSize;

        public List<Chunk> deletionList;

        public HeadChunkIndexWithRtreeMBR indexWithRtreeMBR;

        public List<TrajectoryPoint> pointList;

        @Setup(Level.Trial)
        public void setupData() {
            pointList = SyntheticDataGenerator.generateGaussianDistributionDataSet(listSize, 1, 1);
        }

        @Setup(Level.Invocation)
        public void setupIndex() {
            deletionList = new ArrayList<>();
            indexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR(nodeSize);

            for (int i = 0; i < pointList.size(); i++) {
                TrajectoryPoint point = pointList.get(i);
                indexWithRtreeMBR.updateIndex(point);

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
            String status = indexWithRtreeMBR.printStatus();
            StatusRecorder.recordStatus("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/status/rtree-mbr-deletion-gaussian.log", status);
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
            stateRandom.indexWithRtreeMBR.removeFromIndex(chunk);
        }
        blackhole.consume(stateRandom.indexWithRtreeMBR);
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
            stateGaussian.indexWithRtreeMBR.removeFromIndex(chunk);
        }
        blackhole.consume(stateGaussian.indexWithRtreeMBR);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RTreeMBRDeletionBenchmark.class.getSimpleName())
                .output("/home/yangguo/IdeaProjects/trajectory-index/benchmark-log/headchunk/rtreembr/rtreembr-deletion-1b.log")
                .build();

        new Runner(opt).run();
    }

}
