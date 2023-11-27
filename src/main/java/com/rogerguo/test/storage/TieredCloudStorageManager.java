package com.rogerguo.test.storage;

import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.storage.flush.ToDiskFlushPolicy;
import com.rogerguo.test.storage.flush.ToS3FlushPolicy;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.ImmutableMemoryStorageLayer;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.storage.layer.StorageLayer;
import com.rogerguo.test.store.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.Tier;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author yangguo
 * @create 2021-09-20 10:41 AM
 **/
public class TieredCloudStorageManager {

    List<StorageLayerName> storageLayerHierarchyNameList = new ArrayList<>();  // used to record the sequence of tired storage. the first one is the top (memory), the last one is the bottom (S3)

    Map<StorageLayerName, StorageLayer> storageLayerMap = new HashMap<>();  // the used storage layer should be added here

    Map<String, BlockLocation> blockLocationMappingTable = new HashMap<>(); // record the location of each blocks (not record blocks in S3)

    private SpatialTemporalTree spatialTemporalTree;    // used for recovery

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public TieredCloudStorageManager(ImmutableMemoryStorageLayer immutableMemoryStorageLayer, DiskFileStorageLayer diskFileStorageLayer, ObjectStoreStorageLayer objectStoreStorageLayer) {
        storageLayerMap.put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        storageLayerMap.put(StorageLayerName.EBS, diskFileStorageLayer);
        storageLayerMap.put(StorageLayerName.S3, objectStoreStorageLayer);

        storageLayerHierarchyNameList.add(StorageLayerName.IMMUTABLEMEM);
        storageLayerHierarchyNameList.add(StorageLayerName.EBS);
        storageLayerHierarchyNameList.add(StorageLayerName.S3);

        immutableMemoryStorageLayer.setTieredCloudStorageManager(this);
        diskFileStorageLayer.setTieredCloudStorageManager(this);
        objectStoreStorageLayer.setTieredCloudStorageManager(this);
    }

    public TieredCloudStorageManager() {}

    public void setStorageLayers(StorageConfiguration storageConfiguration) {
        String bucketName = storageConfiguration.getBucketNameForS3();
        Region region = storageConfiguration.getRegionForS3();
        S3LayoutSchema s3LayoutSchema = storageConfiguration.getS3LayoutSchema();
        String pathNameForDiskTier = storageConfiguration.getPathNameForDiskTier();

        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, storageConfiguration.getFlushBlockNumThresholdForMem(), storageConfiguration.getFlushTimeThresholdForMem());
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,pathNameForDiskTier, storageConfiguration.getFlushBlockNumThresholdForDisk(), storageConfiguration.getFlushTimeThresholdForDisk());
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, s3LayoutSchema);

        storageLayerMap.put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        storageLayerMap.put(StorageLayerName.EBS, diskFileStorageLayer);
        storageLayerMap.put(StorageLayerName.S3, objectStoreStorageLayer);

        storageLayerHierarchyNameList.add(StorageLayerName.IMMUTABLEMEM);
        storageLayerHierarchyNameList.add(StorageLayerName.EBS);
        storageLayerHierarchyNameList.add(StorageLayerName.S3);

        immutableMemoryStorageLayer.setTieredCloudStorageManager(this);
        diskFileStorageLayer.setTieredCloudStorageManager(this);
        objectStoreStorageLayer.setTieredCloudStorageManager(this);
    }

    @Deprecated
    public static StorageConfiguration getDefaultStorageConfiguration() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        int flushBlockNumThresholdForMem = 400;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 4;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        return new StorageConfiguration(region, bucketName, flushBlockNumThresholdForMem, flushTimeThresholdForMem, flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
    }

    @Deprecated
    public void initLayersStructureForInMemTest(StorageConfiguration storageConfiguration) {
        String bucketName = storageConfiguration.getBucketNameForS3();
        Region region = storageConfiguration.getRegionForS3();

        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
//        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, 9, 1000 * 60 * 60 * 2);
//        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(layoutSchema);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, storageConfiguration.getFlushBlockNumThresholdForMem(), storageConfiguration.getFlushTimeThresholdForMem());
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(null,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", Integer.MAX_VALUE, Integer.MAX_VALUE);
//        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);

        storageLayerMap.put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        storageLayerMap.put(StorageLayerName.EBS, diskFileStorageLayer);

        storageLayerHierarchyNameList.add(StorageLayerName.IMMUTABLEMEM);
        storageLayerHierarchyNameList.add(StorageLayerName.EBS);
        storageLayerHierarchyNameList.add(StorageLayerName.S3);

        immutableMemoryStorageLayer.setTieredCloudStorageManager(this);
        diskFileStorageLayer.setTieredCloudStorageManager(this);
    }

    @Deprecated
    public void initLayersStructureWithOptimizedS3Flush(StorageConfiguration storageConfiguration) {
        String bucketName = storageConfiguration.getBucketNameForS3();
        Region region = storageConfiguration.getRegionForS3();

        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, 9, 1000 * 60 * 60 * 2);
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(layoutSchema);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, storageConfiguration.getFlushBlockNumThresholdForMem(), storageConfiguration.getFlushTimeThresholdForMem());
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", storageConfiguration.getFlushBlockNumThresholdForDisk(), storageConfiguration.getFlushTimeThresholdForDisk());
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);

        storageLayerMap.put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        storageLayerMap.put(StorageLayerName.EBS, diskFileStorageLayer);
        storageLayerMap.put(StorageLayerName.S3, objectStoreStorageLayer);

        storageLayerHierarchyNameList.add(StorageLayerName.IMMUTABLEMEM);
        storageLayerHierarchyNameList.add(StorageLayerName.EBS);
        storageLayerHierarchyNameList.add(StorageLayerName.S3);

        immutableMemoryStorageLayer.setTieredCloudStorageManager(this);
        diskFileStorageLayer.setTieredCloudStorageManager(this);
        objectStoreStorageLayer.setTieredCloudStorageManager(this);
    }

    @Deprecated
    public void initLayersStructure(StorageConfiguration storageConfiguration) {
        String bucketName = storageConfiguration.getBucketNameForS3();
        Region region = storageConfiguration.getRegionForS3();

        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        S3LayoutSchema layoutSchema = new S3LayoutSchema(S3LayoutSchemaName.DIRECT);
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(layoutSchema);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, storageConfiguration.getFlushBlockNumThresholdForMem(), storageConfiguration.getFlushTimeThresholdForMem());
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", storageConfiguration.getFlushBlockNumThresholdForDisk(), storageConfiguration.getFlushTimeThresholdForDisk());
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, layoutSchema);

        storageLayerMap.put(StorageLayerName.IMMUTABLEMEM, immutableMemoryStorageLayer);
        storageLayerMap.put(StorageLayerName.EBS, diskFileStorageLayer);
        storageLayerMap.put(StorageLayerName.S3, objectStoreStorageLayer);

        storageLayerHierarchyNameList.add(StorageLayerName.IMMUTABLEMEM);
        storageLayerHierarchyNameList.add(StorageLayerName.EBS);
        storageLayerHierarchyNameList.add(StorageLayerName.S3);

        immutableMemoryStorageLayer.setTieredCloudStorageManager(this);
        diskFileStorageLayer.setTieredCloudStorageManager(this);
        objectStoreStorageLayer.setTieredCloudStorageManager(this);
    }



    /**
     * only used for test
     * @param cacheToWhichLayer
     * @param cachedBlockIdList
     */
    public void initLayersCacheDataFromS3(StorageLayerName cacheToWhichLayer, List<String> cachedBlockIdList) {
        StorageLayer storageLayer = storageLayerMap.get(cacheToWhichLayer);
        StorageLayer objectStorageLyaer = storageLayerMap.get(StorageLayerName.S3);
        for (String blockId : cachedBlockIdList) {
            Block block = objectStorageLyaer.get(blockId);
            storageLayer.put(block);
            storageLayer.getLocalLocationMappingTable().get(blockId).setInS3(true);
            getBlockLocationMappingTable().get(blockId).setInS3(true);
        }
    }

    /**
     *
     * @param mode mode 0 is the latest time period mode, where we fill cache by using the block from the latest time period; mode 1 is the random mode where we fill cache by pick up blocks randomly
     * @param percentage
     */
    public void fillDiskLayerCacheDataFromS3(int mode, double percentage) {
        StorageLayer cacheLayer = storageLayerMap.get(StorageLayerName.EBS);
        ObjectStoreStorageLayer objectLayer = (ObjectStoreStorageLayer) storageLayerMap.get(StorageLayerName.S3);

        List<String> blockIdList = objectLayer.getBlockIdListFromObjectStore(mode, percentage);

        int batchSize = 10000;
        for (int i = 0; i < blockIdList.size(); i+= batchSize) {
            int fromIndex = i;
            int toIndex = Math.min(i+batchSize, blockIdList.size());
            List<String> blockIdListInBatch = blockIdList.subList(fromIndex, toIndex);
            List<Block> blockList = objectLayer.batchGet(blockIdListInBatch);
            Map<String, Block> blockMap = new HashMap<>();
            for (Block block : blockList) {
                blockMap.put(block.getBlockId(), block);
            }

            List<BlockIdentifierEntity> entityList = new ArrayList<>();
            for (String string : blockIdListInBatch) {
                BlockIdentifierEntity entity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(string);
                entity.setSpatialPartitionId(entity.getSpatialPointEncoding() / (16384 * 2));
                entityList.add(entity);
            }
            entityList.sort(Comparator.comparingLong(BlockIdentifierEntity::getSpatialPartitionId).thenComparing(BlockIdentifierEntity::getSid));

            for (BlockIdentifierEntity entity : entityList) {
                String blockId = BlockIdentifierEntity.coupleBlockIdForSpatioTemporalLayout(entity);
                cacheLayer.put(blockMap.get(blockId));

            }

            blockMap.clear();
            blockList.clear();
            /*try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            //cacheLayer.batchPut(blockList);
            System.out.println("disk cache fill block index: " + i);
        }
        blockIdList.clear();

    }


    public void put(Block block) {
        logger.info("block size (KB): " + block.getDataString().getBytes(StandardCharsets.UTF_8).length / 1024);
        // 1. decide which layer should be used to put this new block
        StorageLayerName storageLayerName = decideStorageLocationForNewBlock(block);

        // 2. put this block, and update blockLocationMappingTable and mapping table in the corresponding layer
        StorageLayer storageLayer = storageLayerMap.get(storageLayerName);
        storageLayer.put(block);

        // 3. check each layer and find if there exist one needs flush  TODO optimization
        for (StorageLayerName nameInHierarchy : storageLayerHierarchyNameList) {
            StorageLayer storageLayerInHierarchy = storageLayerMap.get(nameInHierarchy);
            if (storageLayerInHierarchy.isFlushNeeded()) {
                storageLayerInHierarchy.flush();
            }
        }


    }

    public Block get(String blockId) {
        if (StoreConfig.IS_USE_GLOBAL_MAPPING_TABLE) {
            if (blockLocationMappingTable.containsKey(blockId)) {
                BlockLocation blockLocation = blockLocationMappingTable.get(blockId);
                if (blockLocation.getStorageLayerName().equals(StorageLayerName.IMMUTABLEMEM)) {
                    return storageLayerMap.get(StorageLayerName.IMMUTABLEMEM).get(blockId);
                } else if (blockLocation.getStorageLayerName().equals(StorageLayerName.EBS)) {
                    return storageLayerMap.get(StorageLayerName.EBS).get(blockId);
                } else {
                    return storageLayerMap.get(StorageLayerName.S3).get(blockId);
                }
            } else {
                return storageLayerMap.get(StorageLayerName.S3).get(blockId);
            }
        } else {
            BlockLocation blockLocation = getBlockLocation(blockId);
            if (blockLocation != null) {
                if (blockLocation.getStorageLayerName().equals(StorageLayerName.IMMUTABLEMEM)) {
                    return storageLayerMap.get(StorageLayerName.IMMUTABLEMEM).get(blockId);
                } else if (blockLocation.getStorageLayerName().equals(StorageLayerName.EBS)) {
                    return storageLayerMap.get(StorageLayerName.EBS).get(blockId);
                } else {
                    return storageLayerMap.get(StorageLayerName.S3).get(blockId);
                }
            } else {
                return storageLayerMap.get(StorageLayerName.S3).get(blockId);
            }
        }

    }

    public BlockLocation getBlockLocation(String blockId) {
        for (StorageLayerName nameInHierarchy : storageLayerHierarchyNameList) {
            StorageLayer storageLayerInHierarchy = storageLayerMap.get(nameInHierarchy);
            if (storageLayerInHierarchy.getLocalLocationMappingTable().containsKey(blockId)) {
                return storageLayerInHierarchy.getLocalLocationMappingTable().get(blockId);
            }
        }
        return null;
    }

    /**
     * group blocks in the same object to reduce the request number to S3
     * for blocks in other tiers, we direct read it at currently
     * @param blockIdList
     * @return
     */
    public List<Block> batchGet(List<String> blockIdList) {
        // TODO need optimization
        List<Block> blockList = new ArrayList<>();


        List<String> blockIdListInObjectStore = new ArrayList<>();
        List<String> blockIdListInDiskTier = new ArrayList<>();
        for (String blockId : blockIdList) {

            if (StoreConfig.IS_USE_GLOBAL_MAPPING_TABLE) {

                if (blockLocationMappingTable.containsKey(blockId)) {
                    BlockLocation blockLocation = blockLocationMappingTable.get(blockId);
                    if (blockLocation.getStorageLayerName().equals(StorageLayerName.IMMUTABLEMEM)) {
                        blockList.add(storageLayerMap.get(StorageLayerName.IMMUTABLEMEM).get(blockId));
                    } else if (blockLocation.getStorageLayerName().equals(StorageLayerName.EBS)) {
                        //blockList.add(storageLayerMap.get(StorageLayerName.EBS).get(blockId));
                        blockIdListInDiskTier.add(blockId);
                    } else {
                        blockIdListInObjectStore.add(blockId);
                    }
                } else {
                    blockIdListInObjectStore.add(blockId);
                }
            } else {
                BlockLocation blockLocation = getBlockLocation(blockId);
                if (blockLocation != null) {
                    if (blockLocation.getStorageLayerName().equals(StorageLayerName.IMMUTABLEMEM)) {
                        blockList.add(storageLayerMap.get(StorageLayerName.IMMUTABLEMEM).get(blockId));
                    } else if (blockLocation.getStorageLayerName().equals(StorageLayerName.EBS)) {
                        blockIdListInDiskTier.add(blockId);
                    } else {
                        blockIdListInObjectStore.add(blockId);
                    }
                } else {
                    blockIdListInObjectStore.add(blockId);
                }
            }
        }

        if (blockIdListInDiskTier.size() > 0) {
            System.out.println("Batch get blocks from disk: " + blockIdListInDiskTier.size());
            DiskFileStorageLayer diskFileStorageLayer = (DiskFileStorageLayer) storageLayerMap.get(StorageLayerName.EBS);
            List<Block> blockListFromDiskTier = diskFileStorageLayer.batchGet(blockIdListInDiskTier);
            blockList.addAll(blockListFromDiskTier);
        }

        if (blockIdListInObjectStore.size() > 0 ) {
            System.out.println("Batch get blocks from S3: " + blockIdListInObjectStore.size());
            ObjectStoreStorageLayer objectStoreStorageLayer = (ObjectStoreStorageLayer) storageLayerMap.get(StorageLayerName.S3);
            List<Block> blockListFromObjectStore = objectStoreStorageLayer.batchGet(blockIdListInObjectStore);
            blockList.addAll(blockListFromObjectStore);

        }


        return blockList;
    }

    public void close() {
        logger.info("closing store");
        for (StorageLayerName storageLayerName : storageLayerHierarchyNameList) {
            StorageLayer storageLayer = storageLayerMap.get(storageLayerName);
            storageLayer.flush();
            storageLayer.close();
        }
        logger.info("finished close and all local data blocks have been moved to S3");

    }

    /**
     * the default way: put new block into immutable memory first
     * @param block
     * @return
     */
    private static StorageLayerName decideStorageLocationForNewBlock(Block block) {
        return StorageLayerName.IMMUTABLEMEM;
    }

    public Map<String, BlockLocation> getBlockLocationMappingTable() {
        return blockLocationMappingTable;
    }

    public Map<StorageLayerName, StorageLayer> getStorageLayerMap() {
        return storageLayerMap;
    }

    public List<StorageLayerName> getStorageLayerHierarchyNameList() {
        return storageLayerHierarchyNameList;
    }

    public SpatialTemporalTree getSpatialTemporalTree() {
        return spatialTemporalTree;
    }

    public void setSpatialTemporalTree(SpatialTemporalTree spatialTemporalTree) {
        this.spatialTemporalTree = spatialTemporalTree;
    }
}
