package com.rogerguo.test.storage.layer;

import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.ToDiskFlushPolicy;
import com.rogerguo.test.util.BlockGenerator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ImmutableMemoryStorageLayerTest {

    private TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();

    @Test
    public void put() {

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(null, 0, 0);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.IMMUTABLEMEM);
        immutableMemoryStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            immutableMemoryStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        System.out.println();
        Block block = immutableMemoryStorageLayer.get("E000.3");

        System.out.println(block);
    }

    @Test
    public void batchPut() {

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(null, 0, 0);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.IMMUTABLEMEM);
        immutableMemoryStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        List<Block> blockList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            blockList.add(BlockGenerator.generateNextBlock());
        }

        immutableMemoryStorageLayer.batchPut(blockList);

        System.out.println();

    }


    @Test
    public void clearAll() {

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(null, 0, 0);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.IMMUTABLEMEM);
        immutableMemoryStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            immutableMemoryStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        System.out.println();

        List<String> blockIdList = new ArrayList<>();
        blockIdList.add("T000.3");
        immutableMemoryStorageLayer.clearAll();

        System.out.println();
    }


    @Test
    public void flush() {
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, 2, 1000 * 60 * 60);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null, "/home/yangguo/IdeaProjects/trajectory-index/flush-test", 0, 0);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.EBS, diskFileStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.IMMUTABLEMEM);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.EBS);
        immutableMemoryStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        diskFileStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 10; i++) {
            immutableMemoryStorageLayer.put(BlockGenerator.generateNextBlock());
        }
        immutableMemoryStorageLayer.flush();

        System.out.println();
    }

}