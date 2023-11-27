package com.rogerguo.test.index;

import com.rogerguo.test.index.predicate.BasicQueryPredicate;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.index.util.IndexTupleGenerator;
import com.rogerguo.test.index.util.TreePrinter;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/3/16 21:22
 * @Created by X1 Carbon
 */
public class SimpleEvaluation {

    public static void main(String[] args) {
        testRebuild();
    }

    public static void testInsertionPerformance() {
        IndexTupleGenerator.generateSyntheticTupleForIndexTest(1000, 0.5, 1);
    }

    public static void testRebuild() {
        SpatialTemporalTree rebuild = rebuildFromS3();
        IdTemporalQueryPredicate idTemporalQueryPredicate = new IdTemporalQueryPredicate(0, 5, "1");
        SpatialTemporalRangeQueryPredicate spatialTemporalPredicate = new SpatialTemporalRangeQueryPredicate(0, 5, new Point(1, 1), new Point(3, 3));

        List<NodeTuple> result = rebuild.searchForSpatialTemporal(spatialTemporalPredicate);
        System.out.println();
        System.out.print(result);
    }

    public static SpatialTemporalTree rebuildFromS3() {
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(SpatialTemporalTree.getDefaultIndexConfiguration());
        SpatialTemporalTree rebuildTree = spatialTemporalTree.loadAndRebuildIndex();

        new TreePrinter(rebuildTree).print(System.out);

        return rebuildTree;
    }

    public static void testFlush2S3() {
        SpatialTemporalTree indexTree = new SpatialTemporalTree(SpatialTemporalTree.getDefaultIndexConfiguration());
        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(10, 1, 5);
        for (TrajectorySegmentMeta meta : metaList) {
            indexTree.insert(meta);
        }

        for (TrajectorySegmentMeta meta : metaList) {
            System.out.println(meta);
        }

        new TreePrinter(indexTree).print(System.out);
        indexTree.serializeAndFlushIndex();
    }

    public static void testIdTemporalQueries() {
        SpatialTemporalTree indexTree = new SpatialTemporalTree(SpatialTemporalTree.getDefaultIndexConfiguration());
        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(10, 1, 5);
        for (TrajectorySegmentMeta meta : metaList) {
            indexTree.insert(meta);
        }

        for (TrajectorySegmentMeta meta : metaList) {
            System.out.println(meta);
        }

        IdTemporalQueryPredicate idTemporalQueryPredicate = new IdTemporalQueryPredicate(0, 5, "1");

        new TreePrinter(indexTree).print(System.out);
        List<NodeTuple> result = indexTree.searchForIdTemporal(idTemporalQueryPredicate);
        System.out.println();
        System.out.print(result);
    }

    public static void testSpatialTemporalQueries() {
        SpatialTemporalTree indexTree = new SpatialTemporalTree(SpatialTemporalTree.getDefaultIndexConfiguration());
        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(10, 1, 5);
        for (TrajectorySegmentMeta meta : metaList) {
            indexTree.insert(meta);
        }

        for (TrajectorySegmentMeta meta : metaList) {
            System.out.println(meta);
        }

        SpatialTemporalRangeQueryPredicate spatialTemporalPredicate = new SpatialTemporalRangeQueryPredicate(0, 5, new Point(1, 1), new Point(3, 3));

        new TreePrinter(indexTree).print(System.out);
        List<NodeTuple> result = indexTree.searchForSpatialTemporal(spatialTemporalPredicate);
        System.out.println();
        System.out.print(result);
    }

    /*public static void testRebuild() {
        SpatialTemporalTree indexTree = new SpatialTemporalTree(2);
        List<TrajectorySegmentMeta> metaList = generateTrajectoryMetaForTest();
        for (TrajectorySegmentMeta meta : metaList) {
            indexTree.insert(meta);
        }
        indexTree.insert(new TrajectorySegmentMeta(11,16, new Point(1, 6), new Point(2, 7),"7", "b6"));
        new TreePrinter(indexTree).print(System.out);
        BasicQueryPredicate predicate = new IdTemporalQueryPredicate(0,5,"2");
        BasicQueryPredicate spatialTemporalPredicate = new SpatialTemporalRangeQueryPredicate(0, 5, new Point(1, 1), new Point(3, 3));
        System.out.println(indexTree.search(spatialTemporalPredicate));

        //SpatialTemporalTree.serializeAndFlushIndex(indexTree);

        System.out.println("\n\n=========================rebuild one==============================");
        SpatialTemporalTree rebuildTree = SpatialTemporalTree.loadAndRebuildIndex("index-tree.meta");
        rebuildTree.insert(new TrajectorySegmentMeta(11,16, new Point(1, 6), new Point(2, 7),"7", "b6"));
        new TreePrinter(rebuildTree).print(System.out);
        BasicQueryPredicate predicateRebuild = new IdTemporalQueryPredicate(0,5,"2");
        BasicQueryPredicate spatialTemporalPredicateRebuild = new SpatialTemporalRangeQueryPredicate(0, 5, new Point(1, 1), new Point(3, 3));
        System.out.println(rebuildTree.search(spatialTemporalPredicateRebuild));
    }*/


/*    public static List<TrajectorySegmentMeta> generateTrajectoryMeta(int size) {
        List<TrajectorySegmentMeta> list = new ArrayList<TrajectorySegmentMeta>();
        for (int i = 0; i < size; i++) {
            TrajectorySegmentMeta meta = new TrajectorySegmentMeta(System.currentTimeMillis(), System.currentTimeMillis()+i, new Point(0, 0), new Point(10, 10), String.valueOf(i), String.valueOf(1));
            list.add(meta);
        }
        return list;
    }*/

/*    public static List<TrajectorySegmentMeta> generateTrajectoryMetaForTest() {
        List<TrajectorySegmentMeta> list = new ArrayList<>();

        TrajectorySegmentMeta meta = new TrajectorySegmentMeta(0,2, new Point(0, 0), new Point(4, 3),"0", "b0");
        list.add(meta);

        TrajectorySegmentMeta meta1 = new TrajectorySegmentMeta(1,4, new Point(0, 0), new Point(2, 4),"1", "b1");
        list.add(meta1);

        TrajectorySegmentMeta meta2 = new TrajectorySegmentMeta(3,7, new Point(1, 0), new Point(3, 3),"2", "b2");
        list.add(meta2);

        TrajectorySegmentMeta meta3 = new TrajectorySegmentMeta(5,8, new Point(7, 9), new Point(9, 10),"3", "b3");
        list.add(meta3);

        TrajectorySegmentMeta meta4 = new TrajectorySegmentMeta(8,10, new Point(3, 3), new Point(4, 9),"2", "b4");
        list.add(meta4);

        TrajectorySegmentMeta meta5 = new TrajectorySegmentMeta(9,12, new Point(3, 0), new Point(10, 2),"5", "b5");
        list.add(meta5);

        TrajectorySegmentMeta meta6 = new TrajectorySegmentMeta(11,13, new Point(1, 6), new Point(2, 7),"6", "b6");
        list.add(meta6);

        return list;
    }*/

/*    public static List<TrajectorySegmentMeta> generateTrajectoryMetaForTest2() {
        List<TrajectorySegmentMeta> list = new ArrayList<>();

        TrajectorySegmentMeta meta = new TrajectorySegmentMeta(0,2, new Point(0, 0), new Point(0, 0),"0", "0");
        list.add(meta);

        TrajectorySegmentMeta meta1 = new TrajectorySegmentMeta(0,2, new Point(0, 0), new Point(0, 0),"1", "0");
        list.add(meta1);

        TrajectorySegmentMeta meta2 = new TrajectorySegmentMeta(0,3, new Point(0, 0), new Point(0, 0),"2", "1");
        list.add(meta2);

        TrajectorySegmentMeta meta3 = new TrajectorySegmentMeta(0,3, new Point(0, 0), new Point(0, 0),"3", "2");
        list.add(meta3);

        TrajectorySegmentMeta meta4 = new TrajectorySegmentMeta(1,3, new Point(0, 0), new Point(0, 0),"4", "2");
        list.add(meta4);

        TrajectorySegmentMeta meta5 = new TrajectorySegmentMeta(2,4, new Point(0, 0), new Point(0, 0),"5", "3");
        list.add(meta5);

        TrajectorySegmentMeta meta6 = new TrajectorySegmentMeta(6,13, new Point(0, 0), new Point(0, 0),"6", "4");
        list.add(meta6);

        return list;
    }*/

    /*public static List<TrajectorySegmentMeta> generateTrajectoryMetaForTestOutOfOrder() {
        List<TrajectorySegmentMeta> list = new ArrayList<>();

        TrajectorySegmentMeta meta = new TrajectorySegmentMeta(0,2, new Point(0, 0), new Point(0, 0),"0", "0");
        list.add(meta);

        TrajectorySegmentMeta meta1 = new TrajectorySegmentMeta(2,5, new Point(0, 0), new Point(0, 0),"1", "1");
        list.add(meta1);

        TrajectorySegmentMeta meta2 = new TrajectorySegmentMeta(1,3, new Point(0, 0), new Point(0, 0),"2", "2");
        list.add(meta2);

        TrajectorySegmentMeta meta3 = new TrajectorySegmentMeta(0,7, new Point(0, 0), new Point(0, 0),"3", "3");
        list.add(meta3);

        TrajectorySegmentMeta meta4 = new TrajectorySegmentMeta(1,3, new Point(0, 0), new Point(0, 0),"4", "4");
        list.add(meta4);

        TrajectorySegmentMeta meta5 = new TrajectorySegmentMeta(2,4, new Point(0, 0), new Point(0, 0),"5", "5");
        list.add(meta5);

        TrajectorySegmentMeta meta6 = new TrajectorySegmentMeta(0,13, new Point(0, 0), new Point(0, 0),"6", "6");
        list.add(meta6);

        return list;
    }*/
}
