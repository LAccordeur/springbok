package com.rogerguo.test.storage;

import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.util.BlockGenerator;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

public class TieredCloudStorageManagerTest {

    private TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();

    @Test
    public void put() {


        tieredCloudStorageManager.initLayersStructure(TieredCloudStorageManager.getDefaultStorageConfiguration());
        for (int i = 0; i < 22; i++) {
            tieredCloudStorageManager.put(BlockGenerator.generateNextBlock());
        }
        tieredCloudStorageManager.close();
    }

    @Test
    public void get() {

        tieredCloudStorageManager.initLayersStructure(TieredCloudStorageManager.getDefaultStorageConfiguration());
        List<String> cachedIdList = new ArrayList<>();
        cachedIdList.add("E000.4");
        tieredCloudStorageManager.initLayersCacheDataFromS3(StorageLayerName.IMMUTABLEMEM, cachedIdList);

        tieredCloudStorageManager.put(BlockGenerator.generateNextBlock());
        Block result = tieredCloudStorageManager.get("E000.4");
        tieredCloudStorageManager.close();
        System.out.println(result);

    }
}