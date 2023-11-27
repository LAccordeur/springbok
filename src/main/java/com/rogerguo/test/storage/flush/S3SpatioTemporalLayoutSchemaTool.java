package com.rogerguo.test.storage.flush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.common.StatisticsTool;
import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.BlockIdentifierEntity;
import com.rogerguo.test.storage.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author yangguo
 * @create 2021-11-01 3:56 PM
 **/
public class S3SpatioTemporalLayoutSchemaTool {

    private static long lastTimePartitionId = -1;

    private static Map<String, Integer> countMap = new HashMap<>();  // TODO remove out-of-date entries

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static List<Integer> objectSizeList = new ArrayList<>();    // for statistic

    private static List<Double> queueSizeAvgList = new ArrayList<>();  // for statistic

    private static List<Double> queueSizeStdDevList = new ArrayList<>();   // for statistic

    private static Logger logger = LoggerFactory.getLogger(S3SpatioTemporalLayoutSchemaTool.class);

    public static String printStatus() {
        Map<String, Double> objectSizeStatistic = StatisticsTool.calculateAverageAndStdDev(objectSizeList);
        Map<String, Double> queueSizeAvgStatistic = StatisticsTool.calculateAverageAndStdDevDouble(queueSizeAvgList);
        Map<String, Double> queueSizeStdDevStatistic = StatisticsTool.calculateAverageAndStdDevDouble(queueSizeStdDevList);
        String status = "[SpatioTemporal Layout] average object size: " + objectSizeStatistic.get("avg") + ", its standard deviation: " + objectSizeStatistic.get("stddev")
                + "\n queue size average: " + queueSizeAvgStatistic.get("avg") + ", its standard deviation: " + queueSizeAvgStatistic.get("stddev")
                + "\n queue size stddev: " + queueSizeStdDevStatistic.get("avg") + ", its standard deviation: " + queueSizeStdDevStatistic.get("stddev");

        return status;
    }

    public static Block assembleBlocksForSpatioTemporalLayout(List<Block> blockList, S3LayoutSchema layoutSchema) {

        logger.info("chunk num in an object: " + blockList.size());
        objectSizeList.add(blockList.size());

        BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockList.get(0).getBlockId());
        long timePartitionId = S3SpatioTemporalLayoutSchemaTool.generateTimePartitionId(blockIdentifierEntity.getTimestamp(), layoutSchema.getTimePartitionLength());
        long spatialPartitionId = S3SpatioTemporalLayoutSchemaTool.generateSpatialPartitionId(blockIdentifierEntity.getSpatialPointEncoding(), layoutSchema.getSpatialRightShiftBitNum());
        String newBlockId = S3SpatioTemporalLayoutSchemaTool.generateObjectKeyForPut(timePartitionId, spatialPartitionId);


        StringBuilder stringBuilder = new StringBuilder();
        Map<String, String> metadataMap = new HashMap<>();
        int length = 0;
        for (Block block : blockList) {
            stringBuilder.append(block.getDataString());
            BlockLocation blockLocation = new BlockLocation(newBlockId, length, block.getDataString().length()); // the range request of s3 is both inclusive
            length = length+block.getDataString().length();
            try {
                metadataMap.put(block.getBlockId(), objectMapper.writeValueAsString(blockLocation));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        Block assembledBlock = null;
        try {
            assembledBlock = new Block(newBlockId, stringBuilder.toString(), objectMapper.writeValueAsString(metadataMap));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return assembledBlock;
    }

    /**
     * all blocks should be from the same partition
     * @param blockIds
     * @param numOfConnectionQueues
     * @param spatialRightShiftBitNum
     * @return
     */
    public static List<Queue<String>> generateQueueList(List<String> blockIds, int numOfConnectionQueues, int spatialRightShiftBitNum) {

        // sort by spatial location, series id, count
        List<BlockIdentifierEntity> identifierEntityList = new ArrayList<>();
        for (String blockId : blockIds) {
            BlockIdentifierEntity entity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
            entity.setSpatialPartitionId(generateSpatialPartitionId(entity.getSpatialPointEncoding(), spatialRightShiftBitNum));
            identifierEntityList.add(entity);
        }

        identifierEntityList.sort(Comparator.comparingLong(BlockIdentifierEntity::getSpatialPartitionId)
                .thenComparing(BlockIdentifierEntity::getSid)
                .thenComparingInt(BlockIdentifierEntity::getCount));

        // generate queue
        List<Queue<String>> queueList = new ArrayList<>(numOfConnectionQueues);
        for (int i = 0; i < numOfConnectionQueues; i++) {
            Queue<String> queue = new LinkedList<>();
            queueList.add(queue);
        }

        for (BlockIdentifierEntity blockIdentifierEntity : identifierEntityList) {
            long spatialPrefix = blockIdentifierEntity.getSpatialPartitionId();
            int index = (int) (spatialPrefix % numOfConnectionQueues);

            queueList.get(index).offer(BlockIdentifierEntity.coupleBlockIdForSpatioTemporalLayout(blockIdentifierEntity));

        }

        List<Integer> queueSizeList = new ArrayList<>();
        logger.info("[SpatioTemporal Layout] queue size: ");
        for (Queue queue : queueList) {
            logger.info(queue.size() + ", ");
            queueSizeList.add(queue.size());
        }
        //System.out.println();
        Map<String, Double> queueSizeStatistic = StatisticsTool.calculateAverageAndStdDev(queueSizeList);
        queueSizeAvgList.add(queueSizeStatistic.get("avg"));
        queueSizeStdDevList.add(queueSizeStatistic.get("stddev"));

        return queueList;
    }

    public static long generateTimePartitionId(long timestamp, int flushPeriod) {
        return timestamp / flushPeriod;
    }

    public static long generateSpatialPartitionId(long spatialEncoding, int spatialRightShiftBitNum) {
        return spatialEncoding >> spatialRightShiftBitNum;
    }

    public static String generateMetaDataObjectKeyForQuery(String blockId, int flushPeriod, int spatialRightShiftBitNum) {
        BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
        long timePartitionId = generateTimePartitionId(blockIdentifierEntity.getTimestamp(), flushPeriod);
        long spatialPartitionId = generateSpatialPartitionId(blockIdentifierEntity.getSpatialPointEncoding(), spatialRightShiftBitNum);
        return timePartitionId + "." + spatialPartitionId;

    }

/*    public static String generateObjectKeyForPut(long timePartitionId, long spatialPartitionId) {
        String objectKey = "";

        if (timePartitionId == lastTimePartitionId) {
            int count;
            String key = String.valueOf(spatialPartitionId);
            if (countMap.containsKey(key)) {
                count = countMap.get(key);
                countMap.put(key, countMap.get(key) + 1);
            } else {
                count = 0;
                countMap.put(key, count+1);
            }
            objectKey = timePartitionId + "." + spatialPartitionId + "." + count;
            lastTimePartitionId = timePartitionId;
        } else {
            countMap = new HashMap<>();
            String key = String.valueOf(spatialPartitionId);
            countMap.put(key, 1);
            lastTimePartitionId = timePartitionId;
            objectKey = timePartitionId + "." + spatialPartitionId + "." + 0;
        }

        return objectKey;
    }*/

    public static String generateObjectKeyForPut(long timePartitionId, long spatialPartitionId) {
        String objectKey = "";

        int count;
        String key = timePartitionId + "." + spatialPartitionId;
        if (countMap.containsKey(key)) {
            count = countMap.get(key);
            countMap.put(key, countMap.get(key) + 1);
        } else {
            count = 0;
            countMap.put(key, count + 1);
        }

        objectKey = timePartitionId + "." + spatialPartitionId + "." + count;
        return objectKey;
    }


}
