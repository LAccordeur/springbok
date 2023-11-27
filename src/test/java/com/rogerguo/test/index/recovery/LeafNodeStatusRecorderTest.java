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

public class LeafNodeStatusRecorderTest {

    IndexConfiguration indexConfiguration = new IndexConfiguration(10, true, "flush-test-1111-from-data", "index-test-recovery", Region.AP_EAST_1, false, false);


    @Test
    public void addFullLeafNode() {
        List<LeafNode> leafNodes = generateTestLeafNodes();
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(indexConfiguration);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }
        System.out.println(recorder);
    }

    @Test
    public void markBlockId() {
        List<LeafNode> leafNodes = generateTestLeafNodes();
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(indexConfiguration);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }
        System.out.println(recorder);

        recorder.markBlockId("b1");
        recorder.markBlockId("b2");
        recorder.markBlockId("b5");
        System.out.println("after marking: ");
        System.out.println(recorder);

    }

    @Test
    public void markBlockIds() {

        List<LeafNode> leafNodes = generateTestLeafNodes();
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(indexConfiguration);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }
        System.out.println(recorder);

        List<String> blockIds = new ArrayList<>();
        blockIds.add("b1");
        blockIds.add("b4");
        recorder.markBlockIds(blockIds);
        System.out.println("after marking: ");
        System.out.println(recorder);
    }

    @Test
    public void checkAndFlushLeafNode() {

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
        System.out.println("after marking: ");
        System.out.println(recorder);
        recorder.checkAndFlushLeafNode();
    }

    @Test
    public void checkAndFlushLeafNodeObject() {
        List<LeafNode> leafNodes = generateTestLeafNodes();
        ObjectStoreDriver objectStoreDriver = new ObjectStoreDriver("flush-test-1111-from-data", Region.AP_EAST_1, "index-test-recovery");
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(objectStoreDriver);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }
        System.out.println(recorder);

        recorder.markBlockId("b1");
        recorder.markBlockId("b2");
        recorder.markBlockId("b3");
        recorder.markBlockId("b4");
        System.out.println("after marking: ");
        System.out.println(recorder);
        recorder.checkAndFlushLeafNode();
    }

    @Test
    public void checkAndFlushLeafNodeDisk() {
        List<LeafNode> leafNodes = generateTestLeafNodes();
        DiskDriver diskDriver = new DiskDriver("/home/yangguo/IdeaProjects/trajectory-index/recovery-test");
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(diskDriver);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }
        System.out.println(recorder);

        recorder.markBlockId("b1");
        recorder.markBlockId("b2");
        recorder.markBlockId("b3");
        recorder.markBlockId("b4");
        System.out.println("after marking: ");
        System.out.println(recorder);
        recorder.checkAndFlushLeafNode();
    }

    @Test
    public void flushLeafNode() {
        List<LeafNode> leafNodes = generateTestLeafNodes();
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(indexConfiguration);
        for (LeafNode node : leafNodes) {
            recorder.addFullLeafNode(node);
        }

        recorder.flushLeafNode(recorder.getFullLeafNodeList().get(0));


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

        return list;
    }
}