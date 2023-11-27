package com.rogerguo.test.index.recovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.index.LeafNode;
import com.rogerguo.test.index.NodeTuple;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.index.util.IndexSerializationUtil;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.driver.PersistenceDriver;
import com.rogerguo.test.storage.layer.DiskFileStorageLayer;
import com.rogerguo.test.storage.layer.StorageLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * @Description
 *
 * To record the tree status for recovery, we need to provides some functions for index tree and flush method
 *
 * for index tree, when a leaf node is full, it will add this leaf node to the recorder
 *
 * for flush method, when it assemble a S3 object and finish flushing, we need to mark entries that have been flushed check the leaf node. If all entries of a leaf node is flushed, then flush that leaf node
 *
 *
 *
 * @Date 2022/5/19 11:29
 * @Created by yangguo
 */
public class LeafNodeStatusRecorder {

    private String mode = "object";  // two options: object tier or disk tier

    private List<LeafNodeStatus> fullLeafNodeList = new LinkedList<>();

    private List<String> checkpointNodeIdList = new ArrayList<>();

    private PersistenceDriver persistenceDriver;

    private static ObjectMapper objectMapper = new ObjectMapper();

    private Set<String> earlyArrivedBlockIdSet = new HashSet<>();  // blocks that flushed before their leaf node is full

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private List<String> fullLeafNodeIdList = new ArrayList<>();

    @Deprecated
    public LeafNodeStatusRecorder(IndexConfiguration configuration) {
        this.persistenceDriver = new ObjectStoreDriver(configuration.getBucketNameInS3(), configuration.getRegionS3(), configuration.getRootDirnameInBucket());
    }

    public LeafNodeStatusRecorder(ObjectStoreDriver objectStoreDriver) {
        this.persistenceDriver = objectStoreDriver;
        this.mode = "object";
    }

    public LeafNodeStatusRecorder(DiskDriver diskDriver) {
        this.persistenceDriver = diskDriver;
        this.mode = "disk";
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper testMapper = new ObjectMapper();
        List<String> testList = new ArrayList<>();
        testList.add("b1");
        testList.add("b2");
        String value = testMapper.writeValueAsString(testList);
        System.out.println(value);
        List<String> re = testMapper.readValue(value, new TypeReference<List<String>>(){});
        System.out.println(re);

    }

    /**
     * when a leaf node is full, call this function
     * @param leafNode
     */
    public void addFullLeafNode(LeafNode leafNode) {
        fullLeafNodeIdList.add(leafNode.getBlockId());
        LeafNodeStatus nodeStatus = new LeafNodeStatus(leafNode);
        fullLeafNodeList.add(nodeStatus);
        logger.info("add leaf node: [{}] to recorder", leafNode.getBlockId());
    }


    /**
     * when a block is flushed, update status by calling this function
     * @param blockId
     */
    public void markBlockId(String blockId) {
        boolean isMatchedOr = false;
        for (LeafNodeStatus nodeStatus : fullLeafNodeList) {
            boolean isMatched = nodeStatus.markFlushedBlocks(blockId);
            isMatchedOr = isMatchedOr | isMatched;
        }
        if (!isMatchedOr) {
            earlyArrivedBlockIdSet.add(blockId);
        }
    }

    public void markBlockIds(List<String> blockIds) {
        Set<String> matchedIdSet = new HashSet<>();
        for (LeafNodeStatus nodeStatus : fullLeafNodeList) {
            List<String> matchedIdList = nodeStatus.markFlushedBlocksBatch(blockIds);
            matchedIdSet.addAll(matchedIdList);
        }

        List<String> notMatchedIdList = new ArrayList<>();
        for (String blockId : blockIds) {
            if (!matchedIdSet.contains(blockId)) {
                notMatchedIdList.add(blockId);
            }
        }
        earlyArrivedBlockIdSet.addAll(notMatchedIdList);

    }

    /**
     * call this function to flush leaf node
     */
    public void checkAndFlushLeafNode() {
        // mark for early arrived block ids
        Iterator<String> earlyArrivedBlockIdIterator = earlyArrivedBlockIdSet.iterator();
        while (earlyArrivedBlockIdIterator.hasNext()) {
            String earlyArrivedBlockId = earlyArrivedBlockIdIterator.next();
            for (LeafNodeStatus nodeStatus : fullLeafNodeList) {
                boolean isMatched = nodeStatus.markFlushedBlocks(earlyArrivedBlockId);
                if (isMatched) {
                    earlyArrivedBlockIdIterator.remove();
                }
            }
        }

        int index;
        for (index = 0; index < fullLeafNodeList.size(); index++) {
            LeafNodeStatus nodeStatus = fullLeafNodeList.get(index);
            if (nodeStatus.checkAllBlockFlushed()) {
                if (index == 0) {
                    flushLeafNode(nodeStatus);
                    logger.info("flush leaf node: [{}] to the persistency storage", nodeStatus.getLeafNode().getBlockId());

                    checkpointNodeIdList.add(nodeStatus.getLeafNode().getBlockId());
                } else {
                    if (fullLeafNodeList.get(index-1).isFlushed()) {
                        flushLeafNode(nodeStatus);
                        logger.info("flush leaf node: [{}] to the persistency storage", nodeStatus.getLeafNode().getBlockId());

                        checkpointNodeIdList.add(nodeStatus.getLeafNode().getBlockId());
                    }
                }

            }
        }

        // remove flushed leafnode
        Iterator<LeafNodeStatus> iterator = fullLeafNodeList.iterator();

        while (iterator.hasNext()) {
            LeafNodeStatus nodeStatus = iterator.next();
            if (checkpointNodeIdList.contains(nodeStatus.getLeafNode().getBlockId())) {
                iterator.remove();
                logger.info("remove flushed leaf node: [{}]", nodeStatus.getLeafNode().getBlockId());
            }
        }


        // save flushed leafnode to checkpoint file
        String filePathPrefix = persistenceDriver.getRootUri();
        String key = generateCheckObjectKey(filePathPrefix);
        String data = "";
        try {
            data = serializeCheckpoint(checkpointNodeIdList);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("error in the checkpoint data: [{}]", checkpointNodeIdList);
        }
        if (persistenceDriver instanceof DiskDriver) {
            File file = new File(key);
            if (file.exists()) {
                file.delete();
            }
        }
        persistenceDriver.flush(key, data);
        logger.info("checkpoint flushed leaf nodes: [{}]", checkpointNodeIdList);

    }

    public static String generateCheckObjectKey(String rootUri) {
        return rootUri + File.separator + "index-checkpoint";
    }

    public static String serializeCheckpoint(List<String> nodeIdList) throws JsonProcessingException {
        return objectMapper.writeValueAsString(nodeIdList);
    }

    public static List<String> deserializeCheckpoint(String value) throws JsonProcessingException {

        return objectMapper.readValue(value, new TypeReference<List<String>>() {});
    }

    public void flushLeafNode(LeafNodeStatus nodeStatus) {
        String filePathPrefix = persistenceDriver.getRootUri();

        // flush leaf node
        LeafNode leafNode = nodeStatus.getLeafNode();
        String temporalIndexString = IndexSerializationUtil.serializeLeafTemporalNode(leafNode.getTemporalIndexNode());
        String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(leafNode.getBlockId());
        persistenceDriver.flush(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);

        String spatialIndexString = IndexSerializationUtil.serializeLeafSpatialNode(leafNode.getSpatialIndexNode());
        String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(leafNode.getBlockId());
        persistenceDriver.flush(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);


        // update node status
        nodeStatus.setFlushed(true);
    }

    @Override
    public String toString() {
        return "LeafNodeStatusRecorder{" +
                "fullLeafNodeList=" + fullLeafNodeList +
                ", checkpointNodeIdList=" + checkpointNodeIdList +
                '}';
    }

    public void printStatus() {
        String status = String.format("total full list: %s;\n checkpointed list: %s; \n not flushed size: %d", fullLeafNodeIdList.toString(), checkpointNodeIdList.toString(), fullLeafNodeList.size());
        System.out.println(status);
    }

    public List<LeafNodeStatus> getFullLeafNodeList() {
        return fullLeafNodeList;
    }

    public void setCheckpointNodeIdList(List<String> checkpointNodeIdList) {
        this.checkpointNodeIdList = checkpointNodeIdList;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}

class LeafNodeStatus {

    private LeafNode leafNode;

    private Set<String> existedBlocks;   // the block ids indexed by this leaf node

    private Set<String> flushedBlocks;  // the block ids that have been flushed

    private boolean flushed;  // true = this leaf node is flushed to S3

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    LeafNodeStatus(LeafNode leafNode) {
        this.leafNode = leafNode;
        this.existedBlocks = new HashSet<>();
        this.flushedBlocks = new HashSet<>();
        for (NodeTuple tuple : leafNode.getTemporalIndexNode().getTuples()) {
            String blockId = tuple.getBlockId();
            existedBlocks.add(blockId);
        }
    }

    /**
     * mark this block has been flushed
     * @param blockId
     */
    boolean markFlushedBlocks(String blockId) {
        boolean isMatched = false;
        if (existedBlocks.contains(blockId)) {
            flushedBlocks.add(blockId);
            logger.info("mark data block: [{}] in leaf node: [{}]", blockId, leafNode.getBlockId());
            isMatched = true;    // there is a match
        }
        return isMatched;
    }

    List<String> markFlushedBlocksBatch(List<String> blockIds) {
        List<String> matchedBlockIdList = new ArrayList<>();

        for (String blockId : blockIds) {
            if (existedBlocks.contains(blockId)) {
                flushedBlocks.add(blockId);
                logger.info("mark data block: [{}] in leaf node: [{}]", blockId, leafNode.getBlockId());
                matchedBlockIdList.add(blockId);
            }
        }
        return matchedBlockIdList;
    }

    boolean checkAllBlockFlushed() {
        if (flushedBlocks.size() == existedBlocks.size()) {
            return true;
        } else {
            return false;
        }
    }

    List<String> getNotFlushedBlockIds() {
        List<String> result = new ArrayList<>();

        for (String existedId : existedBlocks) {
            if (!flushedBlocks.contains(existedId)) {
                result.add(existedId);
            }
        }

        return result;
    }

    boolean isFlushed() {
        return flushed;
    }

    LeafNode getLeafNode() {
        return leafNode;
    }

    void setFlushed(boolean flushed) {
        this.flushed = flushed;
    }

    @Override
    public String toString() {
        return "LeafNodeStatus{" +
                "existedBlocks=" + existedBlocks +
                ", flushedBlocks=" + flushedBlocks +
                ", flushed=" + flushed +
                '}';
    }
}
