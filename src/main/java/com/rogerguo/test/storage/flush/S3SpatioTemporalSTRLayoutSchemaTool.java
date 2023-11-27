package com.rogerguo.test.storage.flush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.common.StatisticsTool;
import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.BlockIdentifierEntity;
import com.rogerguo.test.storage.BlockLocation;
import com.rogerguo.test.util.ZCurve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author yangguo
 * @create 2021-11-17 12:00 PM
 **/
public class S3SpatioTemporalSTRLayoutSchemaTool {

    private static long lastTimePartitionId = -1;

    private static int count = 0;

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static ZCurve zCurve = new ZCurve();

    private static List<Integer> objectSizeList = new ArrayList<>();    // for statistic

    private static List<Double> queueSizeAvgList = new ArrayList<>();  // for statistic

    private static List<Double> queueSizeStdDevList = new ArrayList<>();   // for statistic

    private static Logger logger = LoggerFactory.getLogger(S3SpatioTemporalSTRLayoutSchemaTool.class);

    public static String printStatus() {
        Map<String, Double> objectSizeStatistic = StatisticsTool.calculateAverageAndStdDev(objectSizeList);
        Map<String, Double> queueSizeAvgStatistic = StatisticsTool.calculateAverageAndStdDevDouble(queueSizeAvgList);
        Map<String, Double> queueSizeStdDevStatistic = StatisticsTool.calculateAverageAndStdDevDouble(queueSizeStdDevList);
        String status = "[SpatioTemporal Layout] average object size: " + objectSizeStatistic.get("avg") + ", its standard deviation: " + objectSizeStatistic.get("stddev")
                + "\n queue size average: " + queueSizeAvgStatistic.get("avg") + ", its standard deviation: " + queueSizeAvgStatistic.get("stddev")
                + "\n queue size stddev: " + queueSizeStdDevStatistic.get("avg") + ", its standard deviation: " + queueSizeStdDevStatistic.get("stddev");

        return status;
    }

    /**
     *
     * @param blockIds
     * @param numOfConnectionQueues
     * @param objectSize  the number of blocks in an object
     * @return
     */
    public static List<Queue<String>> generateQueueList(List<String> blockIds, int numOfConnectionQueues, int objectSize) {

        List<BlockIdentifierEntity> identifierEntityList = new ArrayList<>();
        for (String blockId : blockIds) {
            BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
            Map<String, Integer> result = zCurve.from2DCurveValue(blockIdentifierEntity.getSpatialPointEncoding());
            int pointLon = result.get("x") / 256;
            int pointLat = result.get("y") / 256;
            blockIdentifierEntity.setSpatialPointLon(pointLon);
            blockIdentifierEntity.setSpatialPointLat(pointLat);
            identifierEntityList.add(blockIdentifierEntity);
        }

        // sorted by longitude
        identifierEntityList.sort(Comparator.comparingInt(BlockIdentifierEntity::getSpatialPointLon));
        int objectNum = (int) Math.ceil(1.0 * blockIds.size() / objectSize);
        int sliceNum = (int) Math.ceil(Math.sqrt(objectNum));

        List<List<String>> objectsList = new ArrayList<>();
        // partition sorted list to multiple slices and in each slice, we sorted them by latitude and package them
        for (int i = 0; i < sliceNum; i++) {
            int fromIndex = i * sliceNum * objectSize;
            int toIndex = (i + 1) * sliceNum * objectSize;
            fromIndex = Math.min(fromIndex, blockIds.size());
            toIndex = Math.min(toIndex, blockIds.size());
            List<BlockIdentifierEntity> subList = identifierEntityList.subList(fromIndex, toIndex);

            subList.sort(Comparator.comparingInt(BlockIdentifierEntity::getSpatialPointLat).thenComparing(BlockIdentifierEntity::getSid));
            for (int j = 0; j < sliceNum; j++) {
                int startIndex = j * objectSize;
                int endIndex = (j + 1) * objectSize;
                startIndex = Math.min(startIndex, subList.size());
                endIndex = Math.min(endIndex, subList.size());
                List<BlockIdentifierEntity> objectBlockEntityList = subList.subList(startIndex, endIndex);

                List<String> objectBlocks = new ArrayList<>();
                for (BlockIdentifierEntity entity : objectBlockEntityList) {
                    objectBlocks.add(BlockIdentifierEntity.coupleBlockIdForSpatioTemporalLayout(entity));
                }
                objectsList.add(objectBlocks);

            }
        }

        // generate queue
        List<Queue<String>> queueList = new ArrayList<>(numOfConnectionQueues);
        int step = (int) Math.floor(1.0 * objectsList.size() / numOfConnectionQueues);
        for (int i = 0; i < numOfConnectionQueues; i++) {
            Queue<String> queue = new LinkedList<>();
            queueList.add(queue);

            int fromIndex = i * step;
            int toIndex = (i + 1) * step;
            if (i == numOfConnectionQueues - 1) {
                toIndex = objectsList.size();
            }
            List<List<String>> subList = objectsList.subList(fromIndex, toIndex);
            for (List<String> object : subList) {
                queue.addAll(object);
            }

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

    public static Block assembleBlocksForSpatioTemporalLayout(List<Block> blockList, S3LayoutSchema layoutSchema) {
        logger.info("chunk num in an object: " + blockList.size());
        objectSizeList.add(blockList.size());

        BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockList.get(0).getBlockId());
        long timePartitionId = generateTimePartitionId(blockIdentifierEntity.getTimestamp(), layoutSchema.getTimePartitionLength());
        String newBlockId = generateObjectKeyForPut(timePartitionId);


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

    public static long generateTimePartitionId(long timestamp, int flushPeriod) {
        return timestamp / flushPeriod;
    }

/*    public static String generateObjectKeyForPut(long timePartitionId) {
        String objectKey = "";

        if (timePartitionId == lastTimePartitionId) {

            objectKey = timePartitionId + "." + count;
            count++;
            lastTimePartitionId = timePartitionId;
        } else {
            count = 0;
            lastTimePartitionId = timePartitionId;
            objectKey = timePartitionId + "." + count;
            count++;
        }

        return objectKey;
    }*/


    private static Map<String, Integer> objectKeyCountMap = new HashMap<>();

    public static String generateObjectKeyForPut(long timePartitionId) {
        String objectKey = "";

        int count;
        String key = String.valueOf(timePartitionId);
        if (objectKeyCountMap.containsKey(key)) {
            count = objectKeyCountMap.get(key);
            objectKeyCountMap.put(key, objectKeyCountMap.get(key) + 1);
        } else {
            count = 0;
            objectKeyCountMap.put(key, count + 1);
        }
        objectKey = timePartitionId + "." + count;

        return objectKey;
    }

    public static String generateMetaDataObjectKeyForQuery(String blockId, int flushPeriod) {
        BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
        long timePartitionId = generateTimePartitionId(blockIdentifierEntity.getTimestamp(), flushPeriod);
        return timePartitionId + "";

    }

/*    private static long metaLastTimePartitionId = -1;
    private static int metaCount = 0;
    public static String generateMetaDataObjectKeyForPut(String blockId, int flushPeriod) {

        BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
        long timePartitionId = generateTimePartitionId(blockIdentifierEntity.getTimestamp(), flushPeriod);
        String metadataKey = timePartitionId + "";
        if (timePartitionId == metaLastTimePartitionId) {
            metadataKey = metadataKey + "." + metaCount;
            metaCount++;
            metaLastTimePartitionId = timePartitionId;
        } else {
            metaCount = 0;
            metadataKey = metadataKey + "." + metaCount;
            metaCount++;
            metaLastTimePartitionId = timePartitionId;
        }
        return metadataKey;
    }*/


    private static Map<String, Integer> metadataObjectKeyCountMap = new HashMap<>();

    public static String generateMetaDataObjectKeyForPut(String blockId, int flushPeriod) {
        BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
        long timePartitionId = generateTimePartitionId(blockIdentifierEntity.getTimestamp(), flushPeriod);
        String metadataKey = timePartitionId + "";

        int count;
        String key = String.valueOf(timePartitionId);
        if (metadataObjectKeyCountMap.containsKey(key)) {
            count = metadataObjectKeyCountMap.get(key);
            metadataObjectKeyCountMap.put(key, metadataObjectKeyCountMap.get(key) + 1);
        } else {
            count = 0;
            metadataObjectKeyCountMap.put(key, 1);
        }

        metadataKey = metadataKey + "." + count;

        return metadataKey;
    }
}
