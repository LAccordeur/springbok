package com.rogerguo.test.storage;

import com.rogerguo.test.motivation.GetObjects;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.store.Chunk;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2021-09-29 4:11 PM
 **/
public class SimpleEvaluation {

    public static void main(String[] args) {
        String blockId = "T000.4";
        double result = getDirectFromS3TestWithDriver(blockId, 20);
        System.out.println("average latency: " + result);
    }

    public static double getDirectFromS3Test(String blockId, int num) {
        List<Long> latencyList = new ArrayList<>();

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        tieredCloudStorageManager.initLayersStructure(TieredCloudStorageManager.getDefaultStorageConfiguration());

        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            Block result = tieredCloudStorageManager.get(blockId);
            long stop = System.currentTimeMillis();
            latencyList.add((stop - start));
        }
        tieredCloudStorageManager.close();

        System.out.println(latencyList);

        return GetObjects.calculateAverage(latencyList);
    }

    public static double getDirectFromS3TestWithStorageLayer(String blockId, int num) {
        List<Long> latencyList = new ArrayList<>();

        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, new S3LayoutSchema(S3LayoutSchemaName.DIRECT));

        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            Block result = objectStoreStorageLayer.get(blockId);
            long stop = System.currentTimeMillis();
            latencyList.add((stop - start));
        }

        System.out.println(latencyList);

        return GetObjects.calculateAverage(latencyList);
    }

    public static double getDirectFromS3TestWithDriver(String blockId, int num) {
        List<Long> latencyList = new ArrayList<>();

        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreDriver driver = new ObjectStoreDriver(bucketName, region);
        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            String result = driver.getDataAsString(blockId);
            long stop = System.currentTimeMillis();
            latencyList.add((stop - start));
        }

        System.out.println(latencyList);

        return GetObjects.calculateAverage(latencyList);
    }

    public static double getDirectFromMEMTest(String blockId, int num) {
        List<Long> latencyList = new ArrayList<>();

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();

        tieredCloudStorageManager.initLayersStructure(TieredCloudStorageManager.getDefaultStorageConfiguration());
        List<String> cachedIdList = new ArrayList<>();
        cachedIdList.add(blockId);
        tieredCloudStorageManager.initLayersCacheDataFromS3(StorageLayerName.IMMUTABLEMEM, cachedIdList);

        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            Block result = tieredCloudStorageManager.get(blockId);
            long stop = System.currentTimeMillis();
            latencyList.add((stop - start));
        }
        tieredCloudStorageManager.close();
        System.out.println(latencyList);

        return GetObjects.calculateAverage(latencyList);
    }

    public static double getDirectFromDiskTest(String blockId, int num) {
        List<Long> latencyList = new ArrayList<>();

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();

        tieredCloudStorageManager.initLayersStructure(TieredCloudStorageManager.getDefaultStorageConfiguration());
        List<String> cachedIdList = new ArrayList<>();
        cachedIdList.add(blockId);
        tieredCloudStorageManager.initLayersCacheDataFromS3(StorageLayerName.EBS, cachedIdList);

        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            Block result = tieredCloudStorageManager.get(blockId);
            long stop = System.currentTimeMillis();
            latencyList.add((stop - start));
        }
        tieredCloudStorageManager.close();
        System.out.println(latencyList);

        return GetObjects.calculateAverage(latencyList);
    }

}
