package com.rogerguo.test.storage.layer;

import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.FlushPolicy;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.storage.flush.ToS3FlushPolicy;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.util.BlockGenerator;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

public class DiskFileStorageLayerTest {

    private TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();

    @Test
    public void put() {

        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            diskFileStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        System.out.println();

    }

    @Test
    public void get() {
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            diskFileStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        Block block = diskFileStorageLayer.get("E000.4");
        System.out.println(block);

        System.out.println();
    }

    @Test
    public void batchPut() {
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<Block> blockList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            blockList.add(BlockGenerator.generateNextBlock());
        }
        diskFileStorageLayer.batchPut(blockList);

        List<String> blockIds = new ArrayList<>();
        blockIds.add("T000.4");
        blockIds.add("T000.7");
        List<Block> block = diskFileStorageLayer.batchGet(blockIds);
        System.out.println(block);

        System.out.println();

    }


    @Test
    public void clearAll() {

        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            diskFileStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        diskFileStorageLayer.clearAll();

        System.out.println();

    }

    @Test
    public void clear() {
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            diskFileStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        List<String> blockIdList = new ArrayList<>();
        blockIdList.add("E000.1");
        blockIdList.add("E000.2");
        blockIdList.add("E000.3");
        blockIdList.add("E000.6");
        diskFileStorageLayer.clear(blockIdList);

        System.out.println();
    }

    @Test
    public void flushWithDirectSchema() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.DIRECT);
        FlushPolicy flushPolicy = new ToS3FlushPolicy(layoutSchema);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(flushPolicy, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);


        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);

        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            diskFileStorageLayer.put(BlockGenerator.generateNextBlock());
        }
        diskFileStorageLayer.flush();

        System.out.println();
    }

    @Test
    public void flushWithSpatioTemporalSchema() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL);
        FlushPolicy flushPolicy = new ToS3FlushPolicy(layoutSchema);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(flushPolicy, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);


        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);

        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            diskFileStorageLayer.put(BlockGenerator.generateNextBlock());
        }
        diskFileStorageLayer.flush();

        System.out.println(objectStoreStorageLayer.get((String)diskFileStorageLayer.getLocalLocationMappingTable().keySet().toArray()[2]));

        System.out.println();
    }


}