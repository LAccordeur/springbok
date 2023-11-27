package com.rogerguo.test.index.recovery;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.index.LeafNode;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class IndexTreeBuilderTest {

    IndexConfiguration indexConfiguration = new IndexConfiguration(2, true, "flush-test-1111-from-data", "index-test-recovery", Region.AP_EAST_1, false, false);

    ObjectStoreDriver objectStoreDriver = new ObjectStoreDriver("flush-test-1111-from-data", Region.AP_EAST_1, "index-test-recovery");

    DiskDriver diskDriver = new DiskDriver("/home/yangguo/IdeaProjects/trajectory-index/recovery-test");

    @Test
    public void flushTreeLeafNodes() {
        List<LeafNode> leafNodes = generateTestLeafNodes();
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(indexConfiguration);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }
        System.out.println(recorder);

        recorder.markBlockId("b1");
        recorder.markBlockId("b2");
        recorder.markBlockId("b3");
        recorder.markBlockId("b4");
        recorder.markBlockId("b5");
        recorder.markBlockId("b6");
        recorder.markBlockId("b7");
        recorder.markBlockId("b8");
        recorder.markBlockId("b9");
        recorder.markBlockId("b10");
        System.out.println("after marking: ");
        System.out.println(recorder);
        recorder.checkAndFlushLeafNode();
    }

    @Test
    public void rebuildIndexTree() {

        SpatialTemporalTree tree = IndexTreeBuilder.rebuildIndexTree(indexConfiguration);
        System.out.println(tree.printStatus());
        tree.insert(new TrajectorySegmentMeta(21, 24, new Point(22, 22), new Point(25, 25), "11", "b11"));
        tree.insert(new TrajectorySegmentMeta(31, 34, new Point(32, 32), new Point(35, 35), "12", "b12"));
        tree.insert(new TrajectorySegmentMeta(41, 44, new Point(42, 42), new Point(45, 45), "13", "b13"));
        tree.insert(new TrajectorySegmentMeta(51, 54, new Point(52, 52), new Point(55, 55), "14", "b14"));

        System.out.println(tree.printStatus());
    }

    @Test
    public void rebuildIndexTreeObject() {
        SpatialTemporalTree tree = IndexTreeBuilder.rebuildIndexTree(objectStoreDriver, indexConfiguration);
        System.out.println(tree.printStatus());
        tree.insert(new TrajectorySegmentMeta(21, 24, new Point(22, 22), new Point(25, 25), "11", "b11"));
        tree.insert(new TrajectorySegmentMeta(31, 34, new Point(32, 32), new Point(35, 35), "12", "b12"));
        tree.insert(new TrajectorySegmentMeta(41, 44, new Point(42, 42), new Point(45, 45), "13", "b13"));
        tree.insert(new TrajectorySegmentMeta(51, 54, new Point(52, 52), new Point(55, 55), "14", "b14"));

        System.out.println(tree.printStatus());
    }

    @Test
    public void rebuildIndexTreeDisk() {
        SpatialTemporalTree tree = IndexTreeBuilder.rebuildIndexTree(diskDriver, indexConfiguration);
        System.out.println(tree.printStatus());
        tree.insert(new TrajectorySegmentMeta(21, 24, new Point(22, 22), new Point(25, 25), "11", "b11"));
        tree.insert(new TrajectorySegmentMeta(31, 34, new Point(32, 32), new Point(35, 35), "12", "b12"));
        tree.insert(new TrajectorySegmentMeta(41, 44, new Point(42, 42), new Point(45, 45), "13", "b13"));
        tree.insert(new TrajectorySegmentMeta(51, 54, new Point(52, 52), new Point(55, 55), "14", "b14"));

        System.out.println(tree.printStatus());
    }

    private List<LeafNode> generateTestLeafNodes() {
        SpatialTemporalTree tree = new SpatialTemporalTree(indexConfiguration);

        List<LeafNode> list = new ArrayList<>();
        LeafNode leafNode = new LeafNode(null, tree);
        leafNode.insert(new TrajectorySegmentMeta(1, 3, new Point(1, 1), new Point(3, 3), "1", "b1"));
        leafNode.insert(new TrajectorySegmentMeta(1, 4, new Point(2, 2), new Point(5, 5), "2", "b2"));
        list.add(leafNode);

        LeafNode leafNode1 = new LeafNode(null, tree);
        leafNode1.insert(new TrajectorySegmentMeta(6, 9, new Point(4, 4), new Point(7, 6), "3", "b3"));
        leafNode1.insert(new TrajectorySegmentMeta(7, 11, new Point(5, 5), new Point(9, 9), "4", "b4"));
        list.add(leafNode1);

        LeafNode leafNode2 = new LeafNode(null, tree);
        leafNode2.insert(new TrajectorySegmentMeta(11, 15, new Point(7, 7), new Point(9, 9), "5", "b5"));
        leafNode2.insert(new TrajectorySegmentMeta(12, 16, new Point(8, 8), new Point(10, 10), "6", "b6"));
        list.add(leafNode2);

        LeafNode leafNode3 = new LeafNode(null, tree);
        leafNode3.insert(new TrajectorySegmentMeta(13, 17, new Point(9, 9), new Point(12, 12), "7", "b7"));
        leafNode3.insert(new TrajectorySegmentMeta(14, 18, new Point(10, 10), new Point(13, 13), "8", "b8"));
        list.add(leafNode3);

        LeafNode leafNode4 = new LeafNode(null, tree);
        leafNode4.insert(new TrajectorySegmentMeta(17, 20, new Point(12, 15), new Point(14, 17), "9", "b9"));
        leafNode4.insert(new TrajectorySegmentMeta(18, 22, new Point(15, 18), new Point(18, 20), "10", "b10"));
        list.add(leafNode4);

        return list;
    }
}