package com.rogerguo.test.storage.layer;

import com.rogerguo.test.storage.*;
import com.rogerguo.test.storage.flush.FlushPolicy;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * store and read data, maintain meta data in this layer
 * @author yangguo
 * @create 2021-09-20 10:44 AM
 **/
public abstract class StorageLayer {

    private StorageLayerName StorageLayerName;   // like immutable memory, ebs, s3

    private FlushPolicy flushPolicy;    // the policy used to flush data in this layer to the lower layers

    private Map<String, BlockLocation> localLocationMappingTable;  // record which blocks are in this layer

    private long lastFlushTimestamp;

    private int flushBlockNumThreshold;

    private int flushTimeThreshold;

    private int storageBlockNum;

    private TieredCloudStorageManager tieredCloudStorageManager;

    public StorageLayer(FlushPolicy flushPolicy, int flushBlockNumThreshold, int flushTimeThreshold) {
        this.flushPolicy = flushPolicy;
        this.flushBlockNumThreshold = flushBlockNumThreshold;
        this.flushTimeThreshold = flushTimeThreshold;
        this.localLocationMappingTable = new HashMap<>();
        this.storageBlockNum = 0;
        this.lastFlushTimestamp = Long.MIN_VALUE;
    }

    public StorageLayer(FlushPolicy flushPolicy) {
        this.flushPolicy = flushPolicy;
        this.localLocationMappingTable = new HashMap<>();
        this.storageBlockNum = 0;
        this.lastFlushTimestamp = Long.MIN_VALUE;
    }

    public abstract String printStatus();

    /**
     * put a block into this storage layer
     * @param block
     * @return  the storage location in this layer
     */
    public abstract void put(Block block);

    public abstract void batchPut(List<Block> chunkList);

    /**
     * get chunk from this storage layer
     * @param blockId = sid + "." + chunkId for data chunk
     * @return
     */
    public abstract Block get(String blockId);

    public abstract List<Block> batchGet(List<String> blockIdList);

    /**
     * clear all data in this layer, can only be invoked by flush()/flushAll()
     */
    public abstract void clearAll();

    /**
     * clear the specified blocks in this layer, can only be invoked by flush()/flushall()
     * @param blockIdList
     */
    public abstract void clear(List<String> blockIdList);

    public abstract void close();

    public boolean isFlushNeeded() {
        long timeFromLastFlush = System.currentTimeMillis() - getLastFlushTimestamp();
        if (getStorageBlockNum() >= flushBlockNumThreshold || timeFromLastFlush >= flushTimeThreshold) {
            return true;
        } else {
            return false;
        }
    }

    public void flush() {
        if (flushPolicy != null) {
            StorageLayer flushToWhichStorageLayer = tieredCloudStorageManager.getStorageLayerMap().get(flushPolicy.getFlushToWhichStorageLayerName());
            flushPolicy.flush(this, flushToWhichStorageLayer);
        }
    }

    protected void updateLocalLocationMappingTable(String blockId, BlockLocation blockLocation) {
        localLocationMappingTable.put(blockId, blockLocation);
    }

    protected void updateGlobalLocationMappingTable(String blockId, BlockLocation blockLocation) {
        if (StoreConfig.IS_USE_GLOBAL_MAPPING_TABLE) {
            Map<String, BlockLocation> globalMappingTable = tieredCloudStorageManager.getBlockLocationMappingTable();
            globalMappingTable.put(blockId, blockLocation);
        }
    }


    protected BlockLocation getBlockLocation(String blockId) {
        return localLocationMappingTable.get(blockId);
    }

    protected void updateMetaDataByOne() {
        storageBlockNum = storageBlockNum + 1;
    }

    protected void clearLocalLocationMappingTableAll() {
        localLocationMappingTable.clear();
    }

    protected void clearLocalLocationMappingTable(List<String> blockIdList) {
        localLocationMappingTable.keySet().removeIf(blockIdList::contains);
    }


    protected void clearGlobalLocationMappingTable(List<String> blockIdList) {
        tieredCloudStorageManager.getBlockLocationMappingTable().keySet().removeIf(blockIdList::contains);
    }

    protected void clearGlobalLocationMappingTableAll() {
        tieredCloudStorageManager.getBlockLocationMappingTable().clear();
    }

    protected void clearStorageBlockNum() {
        this.storageBlockNum = 0;
    }

    protected void updateStorageBlockNum(int newValue) {
        this.storageBlockNum = newValue;
    }

    public StorageLayerName getStorageLayerName() {
        return StorageLayerName;
    }

    public FlushPolicy getFlushPolicy() {
        return flushPolicy;
    }

    public Map<String, BlockLocation> getLocalLocationMappingTable() {
        return localLocationMappingTable;
    }

    public long getLastFlushTimestamp() {
        return lastFlushTimestamp;
    }

    public int getStorageBlockNum() {
        return storageBlockNum;
    }

    public TieredCloudStorageManager getTieredCloudStorageManager() {
        return tieredCloudStorageManager;
    }

    public void setLastFlushTimestamp(long lastFlushTimestamp) {
        this.lastFlushTimestamp = lastFlushTimestamp;
    }

    public void setTieredCloudStorageManager(TieredCloudStorageManager tieredCloudStorageManager) {
        this.tieredCloudStorageManager = tieredCloudStorageManager;
    }

    public void setStorageLayerName(StorageLayerName storageLayerName) {
        StorageLayerName = storageLayerName;
    }

    public int getFlushBlockNumThreshold() {
        return flushBlockNumThreshold;
    }

    public int getFlushTimeThreshold() {
        return flushTimeThreshold;
    }
}
