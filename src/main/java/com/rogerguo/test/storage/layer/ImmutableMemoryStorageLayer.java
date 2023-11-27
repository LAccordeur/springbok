package com.rogerguo.test.storage.layer;

import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.BlockLocation;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.flush.FlushPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-09-20 11:51 AM
 **/
public class ImmutableMemoryStorageLayer extends StorageLayer {


    private Map<String, Block> immutableMemory;  // // key is block id

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int putCount = 0;

    public ImmutableMemoryStorageLayer(FlushPolicy flushPolicy, int flushBlockNumThreshold, int flushTimeThreshold) {
        super(flushPolicy, flushBlockNumThreshold, flushTimeThreshold);
        this.immutableMemory = new HashMap<>();
        this.setStorageLayerName(StorageLayerName.IMMUTABLEMEM);
    }

    public String printStatus() {
        String status = "[immutable memory layer] flushBlockNumThreshold = " + getFlushBlockNumThreshold() + ", flushTimeThreshold" + getFlushTimeThreshold() +
                "\n # of put requests: " + putCount;

        return status;
    }

    /**
     * Some tasks are done in this put():
     * 1. put data block to this layer
     * 2. update local mapping (used to record which blocks are in this layer) and meta of this layer
     * 3. update global mapping
     * @param block
     */
    @Override
    public void put(Block block) {
        putCount++;

        // 1. put block in the immutable memory region
        String blockId = block.getBlockId();
        immutableMemory.put(block.getBlockId(), block);
        logger.info("[{}] has been put to IMMUTABLEMEM", blockId);

        // 2. update mapping table
        BlockLocation blockLocation = new BlockLocation(this.getStorageLayerName());
        super.updateLocalLocationMappingTable(blockId, blockLocation);
        super.updateGlobalLocationMappingTable(blockId, blockLocation);

        // 3. update meta data of this storage layer
        super.updateMetaDataByOne();
        logger.info("Current block num in IMMUTABLEMEM: [{}]", super.getStorageBlockNum());

    }

    @Override
    public Block get(String blockId){
        logger.info("Get block [{}] from IMMUTABLEMEM", blockId);
        return immutableMemory.get(blockId);
    }


    @Override
    public void batchPut(List<Block> blockList) {
        // TODO need optimization
        for (Block block : blockList) {
            put(block);
        }
    }

    @Override
    public List<Block> batchGet(List<String> blockIdList) {
        List<Block> resultBlockList = new ArrayList<>();
        for (String blockId : blockIdList) {
            resultBlockList.add(get(blockId));
        }
        return resultBlockList;
    }

    /**
     * clear all data, mapping, meta in this layer
     */
    @Override
    public void clearAll() {
        //logger.info("clearAll(): [{}] in IMMUTABLEMEM", getLocalLocationMappingTable().keySet());
        // 1. remove data block
        immutableMemory.clear();

        // 2. remove mapping in both local
        // clearGlobalLocationMappingTable(new ArrayList<>(getLocalLocationMappingTable().keySet()));
        clearLocalLocationMappingTableAll();

        // 3. clear meta
        clearStorageBlockNum();
        logger.info("finish clearAll() of IMMUTABLEMEM");
    }

    @Override
    public void clear(List<String> blockIdList) {
        //logger.info("clear blocks in IMMUTABLEMEM: [{}]", blockIdList);
        // 1. clear data block
        immutableMemory.keySet().removeIf(blockIdList::contains);

        // 2. clear mapping
        clearLocalLocationMappingTable(blockIdList);
        // clearGlobalLocationMappingTable(blockIdList);

        // 3. update meta
        updateStorageBlockNum(getStorageBlockNum() - blockIdList.size());
    }

    @Override
    public void close() {
        // nothing to be done
    }
}
