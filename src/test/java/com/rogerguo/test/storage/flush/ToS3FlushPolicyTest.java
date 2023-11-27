package com.rogerguo.test.storage.flush;

import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.util.BlockGenerator;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ToS3FlushPolicyTest {

    @Test
    public void flushWithSpatioTemporalLayout() {

        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;

        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, 1, 10);
        ToS3FlushPolicy flushPolicy = new ToS3FlushPolicy(layoutSchema);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(flushPolicy, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);

        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<Block> blockList = generateSyntheticBlockForFlush();
        for (Block block : blockList) {
            diskFileStorageLayer.put(block);
        }

        flushPolicy.flushWithSpatioTemporalLayoutParallel(diskFileStorageLayer, objectStoreStorageLayer);

        Block block = objectStoreStorageLayer.get("T003.64.100.1");
        System.out.println("\n\nresult: " + block);
    }

    @Test
    public void flushWithSingleTrajectoryLayout() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;

        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SINGLE_TRAJECTORY, 10);
        ToS3FlushPolicy flushPolicy = new ToS3FlushPolicy(layoutSchema);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(flushPolicy, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);

        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<Block> blockList = generateSyntheticBlockForFlushForSingleTraj();
        for (Block block : blockList) {
            diskFileStorageLayer.put(block);
        }

        flushPolicy.flushWithSingleTrajectoryLayout(diskFileStorageLayer, objectStoreStorageLayer);

        Block block = objectStoreStorageLayer.get("T003.64.2");
        System.out.println("\n\nresult: " + block);
    }

    @Test
    public void flushWithSpatioTemporalLayoutSTR() {

        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;

        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, 10);
        ToS3FlushPolicy flushPolicy = new ToS3FlushPolicy(layoutSchema);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(flushPolicy, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);

        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<Block> blockList = generateSyntheticBlockForFlush();
        for (Block block : blockList) {
            diskFileStorageLayer.put(block);
        }

        flushPolicy.flushWithSpatioTemporalSTRLayout(diskFileStorageLayer, objectStoreStorageLayer);

        Block block = objectStoreStorageLayer.get("T003.64.100.2");
        System.out.println("\n\nresult: " + block);
    }

    private List<Block> generateSyntheticBlockForFlushForSingleTraj() {
        List<Block> blockList = new ArrayList<>();

        String id1 = "T000.64.1";
        String id2 = "T003.64.2";
        String id3 = "T003.64.1";
        String id4 = "T000.64.2";
        String id5 = "T006.64.1";
        String id6 = "T006.64.2";
        String id7 = "T008.64.1";
        String id8 = "T008.64.2";
        String id9 = "T011.64.1";
        String id10 = "T011.64.2";
        String id11 = "T012.64.1";
        String id12 = "T012.64.2";
        String id13 = "T013.64.0";

        Block block1 = new Block(id1, "data1");
        blockList.add(block1);
        Block block2 = new Block(id2, "data2");
        blockList.add(block2);
        Block block3 = new Block(id3, "data3");
        blockList.add(block3);

        Block block4 = new Block(id4, "data4");
        blockList.add(block4);
        Block block5 = new Block(id5, "data5");
        blockList.add(block5);
        Block block6 = new Block(id6, "data6");
        blockList.add(block6);

        Block block7 = new Block(id7, "data7");
        blockList.add(block7);
        Block block8 = new Block(id8, "data8");
        blockList.add(block8);
        Block block9 = new Block(id9, "data9");
        blockList.add(block9);

        Block block10 = new Block(id10, "data10");
        blockList.add(block10);
        Block block11 = new Block(id11, "data11");
        blockList.add(block11);
        Block block12 = new Block(id12, "data12");
        blockList.add(block12);

        Block block13 = new Block(id13, "data13");
        blockList.add(block13);
        return blockList;
    }

    private List<Block> generateSyntheticBlockForFlush() {
        List<Block> blockList = new ArrayList<>();

        String id1 = "T000.64.100.1";
        String id2 = "T003.64.100.2";
        String id3 = "T003.64.100.1";
        String id4 = "T000.64.100.2";
        String id5 = "T006.64.102.1";
        String id6 = "T006.64.102.2";
        String id7 = "T008.64.102.1";
        String id8 = "T008.64.102.2";
        String id9 = "T011.64.104.1";
        String id10 = "T011.64.104.2";
        String id11 = "T012.64.104.1";
        String id12 = "T012.64.104.2";
        String id13 = "T013.64.106.0";

        Block block1 = new Block(id1, "data1");
        blockList.add(block1);
        Block block2 = new Block(id2, "data2");
        blockList.add(block2);
        Block block3 = new Block(id3, "data3");
        blockList.add(block3);

        Block block4 = new Block(id4, "data4");
        blockList.add(block4);
        Block block5 = new Block(id5, "data5");
        blockList.add(block5);
        Block block6 = new Block(id6, "data6");
        blockList.add(block6);

        Block block7 = new Block(id7, "data7");
        blockList.add(block7);
        Block block8 = new Block(id8, "data8");
        blockList.add(block8);
        Block block9 = new Block(id9, "data9");
        blockList.add(block9);

        Block block10 = new Block(id10, "data10");
        blockList.add(block10);
        Block block11 = new Block(id11, "data11");
        blockList.add(block11);
        Block block12 = new Block(id12, "data12");
        blockList.add(block12);

        Block block13 = new Block(id13, "data13");
        blockList.add(block13);
        return blockList;
    }

    @Test
    public void assembleBlocksForSpatioTemporalLayout() {
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, 2, 10);
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema);

        List<Block> blockList = generateSyntheticBlocks();
        Block block = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockList, s3LayoutSchema);

        System.out.println(block);

        List<Block> blockList1 = generateSyntheticBlocks();
        Block block1 = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockList1, s3LayoutSchema);

        System.out.println(block1);

        List<Block> blockList2 = generateSyntheticBlocks();
        Block block2 = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockList2, s3LayoutSchema);

        System.out.println(block2);
    }

    private List<Block> generateSyntheticBlocks() {

        List<Block> blockList = new ArrayList<>();

        Block block1 = new Block("T000.100.64.1", "data1");
        blockList.add(block1);
        Block block2 = new Block("T000.100.64.2", "data2");
        blockList.add(block2);
        Block block3 = new Block("T000.101.64.1", "data3");
        blockList.add(block3);

        return blockList;
    }
}