package com.rogerguo.test.storage.flush.task;

import com.rogerguo.test.index.recovery.LeafNodeStatusRecorder;
import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.BlockIdentifierEntity;
import com.rogerguo.test.storage.flush.S3SpatioTemporalLayoutSchemaTool;
import com.rogerguo.test.storage.flush.ToS3FlushPolicy;
import com.rogerguo.test.storage.layer.ObjectStoreStorageLayer;
import com.rogerguo.test.storage.layer.StorageLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

/**
 * @author yangguo
 * @create 2021-11-03 5:40 PM
 **/
public class S3FlushTaskForSpatioTemporal implements Runnable{

    private CountDownLatch countDownLatch;

    private Queue<String> blockIdQueue;

    private StorageLayer storageLayerNeededFlush;

    private StorageLayer objectStoreStorageLayer;

    private ToS3FlushPolicy toS3FlushPolicy;

    private int objectSize; // the number of blocks in each object

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public S3FlushTaskForSpatioTemporal(CountDownLatch countDownLatch, Queue<String> blockIdQueue, StorageLayer storageLayerNeededFlush, StorageLayer objectStoreStorageLayer, ToS3FlushPolicy toS3FlushPolicy, int objectSize) {
        this.countDownLatch = countDownLatch;
        this.blockIdQueue = blockIdQueue;
        this.storageLayerNeededFlush = storageLayerNeededFlush;
        this.objectStoreStorageLayer = objectStoreStorageLayer;
        this.toS3FlushPolicy = toS3FlushPolicy;
        this.objectSize = objectSize;
    }

    @Override
    public void run() {
        logger.info(Thread.currentThread().getName() + " is running");

        // for recovery
        LeafNodeStatusRecorder leafNodeStatusRecorder = toS3FlushPolicy.getLeafNodeStatusRecorder();

        //System.out.println(blockIdQueue);
        List<String> blockIdsInObject = new ArrayList<>();
        long lastSpatialPrefix = -1;
        while(!blockIdQueue.isEmpty()) {
            String blockId = blockIdQueue.poll();
            BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
            blockIdentifierEntity.setSpatialPartitionId(S3SpatioTemporalLayoutSchemaTool.generateSpatialPartitionId(blockIdentifierEntity.getSpatialPointEncoding(), toS3FlushPolicy.getLayoutSchema().getSpatialRightShiftBitNum()));

            // check if it is a new  spatial partition
            if (blockIdentifierEntity.getSpatialPartitionId() != lastSpatialPrefix && lastSpatialPrefix != -1) {

                if (blockIdsInObject.size() > 0) {
                    List<Block> blockListInObject = new ArrayList<>();
                    for (String blockIdInObject : blockIdsInObject) {
                        blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                    }
                    Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, toS3FlushPolicy.getLayoutSchema());
                    //System.out.println(assembledBlock);
                    objectStoreStorageLayer.put(assembledBlock);

                    // for recovery
                    if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                        leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                    }

                    // reset
                    blockIdsInObject.clear();
                }

                blockIdsInObject.add(blockId);
                lastSpatialPrefix = blockIdentifierEntity.getSpatialPartitionId();

                if (blockIdsInObject.size() >= objectSize) {
                    // only go here where each object contains one chunk
                    // assemble and flush
                    List<Block> blockListInObject = new ArrayList<>();
                    for (String blockIdInObject : blockIdsInObject) {
                        blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                    }
                    Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, toS3FlushPolicy.getLayoutSchema());
                    //System.out.println(assembledBlock);
                    objectStoreStorageLayer.put(assembledBlock);

                    // for recovery
                    if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                        leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                    }

                    // reset
                    blockIdsInObject.clear();
                }
                continue;
            }

            // if not a new partition, check whether it is full
            if (blockIdsInObject.size() >= (objectSize - 1)) {
                blockIdsInObject.add(blockId);
                lastSpatialPrefix = blockIdentifierEntity.getSpatialPartitionId();
                // assemble and flush
                List<Block> blockListInObject = new ArrayList<>();
                for (String blockIdInObject : blockIdsInObject) {
                    blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                }
                Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, toS3FlushPolicy.getLayoutSchema());
                //System.out.println(assembledBlock);
                objectStoreStorageLayer.put(assembledBlock);

                // for recovery
                if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                    leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                }

                // reset
                blockIdsInObject.clear();
            } else {
                blockIdsInObject.add(blockId);
                lastSpatialPrefix = blockIdentifierEntity.getSpatialPartitionId();
            }

        }

        // the remaining ids must come from the same spatial partition
        if (blockIdsInObject.size() > 0) {
            List<Block> blockListInObject = new ArrayList<>();
            for (String blockIdInObject : blockIdsInObject) {
                blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
            }
            Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, toS3FlushPolicy.getLayoutSchema());
            //System.out.println(assembledBlock);
            objectStoreStorageLayer.put(assembledBlock);
            // for recovery
            if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
            }

            // reset
            blockIdsInObject.clear();
        }

        // for recovery
        if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
            leafNodeStatusRecorder.checkAndFlushLeafNode();
        }

        countDownLatch.countDown();

    }

}
