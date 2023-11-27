package com.rogerguo.test.storage.layer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.storage.*;
import com.rogerguo.test.storage.aws.AWSS3Driver;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.driver.PersistenceDriver;
import com.rogerguo.test.storage.flush.*;
import com.rogerguo.test.store.StoreConfig;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.util.*;

/**
 * we store blocks in the object store such as AWS S3
 * @author yangguo
 * @create 2021-09-20 9:15 PM
 **/
public class ObjectStoreStorageLayer extends StorageLayer {

    private ObjectStoreDriver objectStoreDriver;

    private String bucketName;

    private Region region;

    private ObjectMapper objectMapper;

    private S3LayoutSchema s3LayoutSchema;

    private Map<String, List<String>> simplePrefixKeyListCache = new HashMap<>();   // (for incremental cache option)

    private Map<String, String> simpleMetaObjectCache = new HashMap<>(); // key is metadata object key, value is metadata (for incremental cache option)

    private Map<String, Map<String, String>> simpleMetadataCache = new HashMap<>(); // key is metadata object key, value is deserialized metadata (for start-up cache option) (for mem option)

    private Map<String, String> simpleMetadataCacheDisk = new HashMap<>();  // key is metadata object key, value is the file path of metadata in disk (for start-up cache option) (for disk option)

    private Map<String, Long> objectSizeMap = new HashMap<>();

    private Map<String, String> simpleDataObjectCache = new HashMap<>(); // key is data object key

    private Queue<String> dataObjectCacheQueue = new LinkedList<>();  // used to kick out data object

    private int dataCacheSize = StoreConfig.OBJECT_STORE_DATA_OBJECT_CACHE_SIZE;

    private String simpleMetaCacheStorageOption = StoreConfig.META_OBJECT_CACHE_INIT_STORAGE_OPTION;

    private PersistenceDriver persistenceDriver;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int putCount = 0;

    public ObjectStoreStorageLayer(FlushPolicy flushPolicy, String bucketName, Region region, S3LayoutSchema layoutSchema) {
        super(flushPolicy);
        this.bucketName = bucketName;
        this.region = region;
        this.objectStoreDriver = new ObjectStoreDriver(bucketName, region);
        this.objectMapper = new ObjectMapper();
        this.s3LayoutSchema = layoutSchema;
        this.setStorageLayerName(StorageLayerName.S3);

        if (StoreConfig.ENABLE_META_OBJECT_CACHE_INIT) {
            this.persistenceDriver = new DiskDriver("/home/ubuntu/data/metadata-test");
            initMetaDataObjectCache();

        }

    }

    public String printStatus() {
        String status = "[Object Storage Layer] S3 layout scheme = " + s3LayoutSchema.toString() + ", flushBlockNumThreshold = " + getFlushBlockNumThreshold() + ", flushTimeThreshold" + getFlushTimeThreshold() +
                "\n # of put requests (# of objects): " + putCount;
        return status;
    }

    @Override
    public boolean isFlushNeeded() {
        return false;
    }

    /**
     * invoke by flush()
     * @param block
     */
    @Override
    public void put(Block block) {

        // 1. put to AWS S3
        String blockId = block.getBlockId();
        if (block.getMetaDataString() == null) {
            putCount++;
            objectStoreDriver.flush(blockId, block.getDataString());
            logger.info("[{}] has been put to S3", blockId);
        } else {

            objectStoreDriver.flush(blockId, block.getDataString());
            objectStoreDriver.flush(blockId+".mapping", block.getMetaDataString());
            putCount = putCount + 2;

            logger.info("[{}] has been put to S3", blockId);
            logger.info("[{}] has been put to S3", blockId+".mapping");
        }


    }

    static int dataCacheCount = 0;
    static int dataS3PartialCount = 0;
    static int dataS3FullCount = 0;
    static Set<String> uniqueObjectKeySet = new HashSet<>();

    @Override
    public Block get(String blockId) {

        Block block = new Block();
        String blockString;
        BlockLocation blockLocation = getBlockLocation(blockId);
        uniqueObjectKeySet.add(blockLocation.getFilepath());
        //System.out.println(blockLocation);
        if (!blockLocation.isRange()) {

            if (simpleDataObjectCache.containsKey(blockLocation.getFilepath())) {
                blockString = simpleDataObjectCache.get(blockLocation.getFilepath());
                logger.info("Get block [{}] from simple data cache", blockLocation.getFilepath());
            } else {
                blockString = objectStoreDriver.getDataAsString(blockLocation.getFilepath());
                logger.info("Get block [{}] from S3", blockLocation.getFilepath());
                if (StoreConfig.ENABLE_OBJECT_STORE_DATA_OBJECT_CACHE) {
                    if (simpleDataObjectCache.keySet().size() > dataCacheSize) {
                        String key = dataObjectCacheQueue.poll();
                        simpleDataObjectCache.remove(key);
                    }
                    simpleDataObjectCache.put(blockLocation.getFilepath(), blockString);
                    dataObjectCacheQueue.offer(blockLocation.getFilepath());
                }
            }
        } else {
            //System.out.println(blockLocation);
            if (simpleDataObjectCache.containsKey(blockLocation.getFilepath())) {
                blockString = simpleDataObjectCache.get(blockLocation.getFilepath());
                blockString = blockString.substring(blockLocation.getOffset(), blockLocation.getOffset() + blockLocation.getLength());
                logger.info("Get block [{}] from simple data cache", blockLocation.getFilepath());
                //System.out.println("data cache");
                dataCacheCount++;
            } else {
                long objectSize = 0;
                if (StoreConfig.ENABLE_OBJECT_STORE_PARTIAL_GET) {
                    objectSize = getObjectSize(blockLocation.getFilepath());
                }
                //System.out.println("object size: " + objectSize);
                if (objectSize > 1024 * 1024 * 64) {

                    blockString = objectStoreDriver.getDataAsStringPartial(blockLocation.getFilepath(), blockLocation.getOffset(), blockLocation.getLength());
                    logger.info("Get block [{}] from S3 in a partial way", blockLocation.getFilepath());
                    dataS3PartialCount++;
                } else {
                    blockString = objectStoreDriver.getDataAsString(blockLocation.getFilepath());
                    logger.info("Get block [{}] from S3 in a full way", blockLocation.getFilepath());
                    if (StoreConfig.ENABLE_OBJECT_STORE_DATA_OBJECT_CACHE) {
                        if (simpleDataObjectCache.keySet().size() > dataCacheSize) {
                            String key = dataObjectCacheQueue.poll();
                            simpleDataObjectCache.remove(key);
                        }

                        simpleDataObjectCache.put(blockLocation.getFilepath(), blockString);
                        dataObjectCacheQueue.offer(blockLocation.getFilepath());
                    }
                    blockString = blockString.substring(blockLocation.getOffset(), blockLocation.getOffset() + blockLocation.getLength());
                    dataS3FullCount++;

                }
            }
        }
        block.setBlockId(blockId);
        block.setDataString(blockString);

        // System.out.println("data cache count: " + dataCacheCount);
        // System.out.println("s3 partial count: " + dataS3PartialCount);
        // System.out.println("s3 full count: " + dataS3FullCount);
        // System.out.println("unique object size: " + uniqueObjectKeySet.size());

        return block;
    }


    private long getObjectSize(String key) {
        if (objectSizeMap.containsKey(key)) {
            return objectSizeMap.get(key);
        } else {
            long size = objectStoreDriver.getObjectSize(key);
            objectSizeMap.put(key, size);
            return size;
        }
    }

    @Override
    public BlockLocation getBlockLocation(String blockId) {

        // since we need to access metadata object to get location, to reduce S3 access overhead, we cache metadata object locally. There are two options
        // 1. we cache all metadata object when the store starts
        // 2. we cache them in an incremental way that is the object will be cached after its first access

        if ((StoreConfig.ENABLE_META_OBJECT_CACHE_INIT && !simpleMetadataCache.isEmpty()) || (StoreConfig.ENABLE_META_OBJECT_CACHE_INIT && !simpleMetadataCacheDisk.isEmpty())) {
            return getBlockLocationByLayoutSchema(blockId);
        } else {
            if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.DIRECT)) {
                return new BlockLocation(blockId, false);
            } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
                return getBlockLocationForSingleTrajectoryLayout(blockId);
            } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
                return getBlockLocationForSpatioTemporalLayout(blockId);
            } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
                return getBlockLocationForSpatioTemporalSTRLayout(blockId);
            } else {
                throw new UnsupportedOperationException("please specify s3 layout schema");
            }
        }

    }

    public List<String> getBlockIdListFromObjectStore(int mode, double percentage) {
        List<String> blockIdList = new ArrayList<>();

        List<String> keys = objectStoreDriver.listKeysInBuckets();
        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keys) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }

        Collections.sort(metadataKeyList);

        List<String> pickupMetadataKeyList = new ArrayList<>();
        if (mode == 0) {
            // latest mode: pick up the latest block id
            int size = metadataKeyList.size();
            int fromIndex = (int) Math.round(metadataKeyList.size() * (1 - percentage));
            pickupMetadataKeyList = metadataKeyList.subList(fromIndex, size);

        } else if (mode == 1) {
            // random mode: pick up the random block id
            int gap;
            if (percentage == 0) {
                gap = metadataKeyList.size() ;
            } else {
                gap = (int) Math.round((metadataKeyList.size() / (metadataKeyList.size() * percentage)));
            }
            for (int i = 0; i < metadataKeyList.size(); i+=gap) {
                pickupMetadataKeyList.add(metadataKeyList.get(i));
            }

        }

        System.out.println("total metadata object number: " + metadataKeyList.size());
        System.out.println("pickup metadata object number: " + pickupMetadataKeyList.size());
        Map<String,String> metadataStringMap = objectStoreDriver.getDataAsStringBatch(pickupMetadataKeyList);
        for (String key : pickupMetadataKeyList) {
            String metadata = metadataStringMap.get(key);
            if (metadata != null && metadata != "") {
                Map<String, String> blockLocationMap = null;
                try {
                    blockLocationMap = objectMapper.readValue(metadata, Map.class);
                    blockIdList.addAll(blockLocationMap.keySet());
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

            }
        }
        System.out.println("pickup block number: " + blockIdList.size());

        return blockIdList;
    }

    private void initMetaDataObjectCache() {
        System.out.println("Init object metadata cache...");
        List<String> keys = objectStoreDriver.listKeysInBuckets();
        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keys) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }

        System.out.println("metadata object number: " + metadataKeyList.size());
        Map<String,String> metadataStringMap = objectStoreDriver.getDataAsStringBatch(metadataKeyList);
        for (String key : metadataStringMap.keySet()) {
            String metadata = metadataStringMap.get(key);
            if (metadata != null && metadata != "") {
                Map<String, String> blockLocationMap = null;
                try {
                    if ("disk".equals(simpleMetaCacheStorageOption)) {
                        String filepath = persistenceDriver.getRootUri() + File.separator + key;
                        simpleMetadataCacheDisk.put(key, filepath);
                        persistenceDriver.flush(filepath, metadata);
                    } else {
                        blockLocationMap = objectMapper.readValue(metadata, Map.class);
                        simpleMetadataCache.put(key, blockLocationMap);
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

            }
        }
        System.out.println("Init object metadata cache done. The meta object number: " + metadataKeyList.size());
    }

    private BlockLocation getBlockLocationByLayoutSchema(String blockId) {
        String newBlockIdPrefix = "";
        if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.DIRECT)) {
            return new BlockLocation(blockId, false);
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
            newBlockIdPrefix = S3SingleTrajectoryLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength());
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
            newBlockIdPrefix = S3SpatioTemporalLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength(), s3LayoutSchema.getSpatialRightShiftBitNum());
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
            newBlockIdPrefix = S3SpatioTemporalSTRLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength());
        } else {
            throw new UnsupportedOperationException("please specify s3 layout schema");
        }

        BlockLocation blockLocation = null;
        if ("disk".equals(simpleMetaCacheStorageOption)) {
            for (String metadataKey : simpleMetadataCacheDisk.keySet()) {
                if (metadataKey.startsWith(newBlockIdPrefix)) {
                    String filepath = simpleMetadataCacheDisk.get(metadataKey);
                    String metaString = persistenceDriver.getDataAsString(filepath);

                    Map<String, String> blockLocationMap = null;
                    try {
                        blockLocationMap = objectMapper.readValue(metaString, Map.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    if (blockLocationMap != null && blockLocationMap.containsKey(blockId)) {
                        String blockLocationString = blockLocationMap.get(blockId);
                        try {
                            blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }


        } else {

            for (String metadataKey : simpleMetadataCache.keySet()) {
                if (metadataKey.startsWith(newBlockIdPrefix)) {
                    Map<String, String> blockLocationMap = simpleMetadataCache.get(metadataKey);
                    if (blockLocationMap.containsKey(blockId)) {
                        String blockLocationString = blockLocationMap.get(blockId);
                        try {
                            blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }
        }

        if (blockLocation != null) {
            blockLocation.setStorageLayerName(StorageLayerName.S3);
            blockLocation.setRange(true);
        } else {
            System.err.println("Not find a location in S3");
        }
        return blockLocation;
    }

    private BlockLocation getBlockLocationForSingleTrajectoryLayout(String blockId) {
        String newBlockIdPrefix = S3SingleTrajectoryLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength());
        // get metadata of this object
        List<String> keyList;
        if (simplePrefixKeyListCache.containsKey(newBlockIdPrefix)) {
            keyList = simplePrefixKeyListCache.get(newBlockIdPrefix);
        } else {
            keyList = objectStoreDriver.listKeysWithSamePrefix(newBlockIdPrefix);
            simplePrefixKeyListCache.put(newBlockIdPrefix, keyList);
        }
        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keyList) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }

        BlockLocation blockLocation = null;
        for (String metadataKey : metadataKeyList) {
            //String metadata = objectStoreDriver.getDataAsString(metadataKey);
            String metadata = "";
            if (simpleMetaObjectCache.containsKey(metadataKey)) {
                metadata = simpleMetaObjectCache.get(metadataKey);
                logger.info("get this metadata [{}] from simple meta object cache", metadataKey);
            } else {
                metadata = objectStoreDriver.getDataAsString(metadataKey);
                simpleMetaObjectCache.put(metadataKey, metadata);
            }

            Map<String, String> blockLocationMap = null;
            try {
                blockLocationMap = objectMapper.readValue(metadata, Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (blockLocationMap.containsKey(blockId)) {
                String blockLocationString = blockLocationMap.get(blockId);
                try {
                    blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        blockLocation.setStorageLayerName(StorageLayerName.S3);
        blockLocation.setRange(true);
        return blockLocation;
    }

    private BlockLocation getBlockLocationForSpatioTemporalSTRLayout(String blockId) {
        String newBlockIdPrefix = S3SpatioTemporalSTRLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength());
        // get metadata of this object
        List<String> keyList;
        if (simplePrefixKeyListCache.containsKey(newBlockIdPrefix)) {
            keyList = simplePrefixKeyListCache.get(newBlockIdPrefix);
        } else {
            keyList = objectStoreDriver.listKeysWithSamePrefix(newBlockIdPrefix);
            simplePrefixKeyListCache.put(newBlockIdPrefix, keyList);
        }

        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keyList) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }

        BlockLocation blockLocation = null;
        for (String metadataKey : metadataKeyList) {
            //String metadata = objectStoreDriver.getDataAsString(metadataKey);
            String metadata = "";
            if (simpleMetaObjectCache.containsKey(metadataKey)) {
                metadata = simpleMetaObjectCache.get(metadataKey);
                logger.info("get this metadata [{}] from simple meta object cache", metadataKey);
            } else {
                metadata = objectStoreDriver.getDataAsString(metadataKey);
                simpleMetaObjectCache.put(metadataKey, metadata);
            }

            Map<String, String> blockLocationMap = null;
            try {
                blockLocationMap = objectMapper.readValue(metadata, Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (blockLocationMap.containsKey(blockId)) {
                String blockLocationString = blockLocationMap.get(blockId);
                try {
                    blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        blockLocation.setStorageLayerName(StorageLayerName.S3);
        blockLocation.setRange(true);
        return blockLocation;
    }

    private BlockLocation getBlockLocationForSpatioTemporalLayout(String blockId) {

        logger.info(blockId);
        String newBlockIdPrefix = S3SpatioTemporalLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength(), s3LayoutSchema.getSpatialRightShiftBitNum());
        // get metadata of this object
        List<String> keyList;
        if (simplePrefixKeyListCache.containsKey(newBlockIdPrefix)) {
            keyList = simplePrefixKeyListCache.get(newBlockIdPrefix);
        } else {
            keyList = objectStoreDriver.listKeysWithSamePrefix(newBlockIdPrefix);
            simplePrefixKeyListCache.put(newBlockIdPrefix, keyList);
        }

        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keyList) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }
        logger.info("metadata object number: " + metadataKeyList.size() + ", value: " + metadataKeyList);
        BlockLocation blockLocation = null;
        for (String metadataKey : metadataKeyList) {
            String metadata = "";
            if (simpleMetaObjectCache.containsKey(metadataKey)) {
                metadata = simpleMetaObjectCache.get(metadataKey);
                logger.info("get this metadata [{}] from simple meta object cache", metadataKey);
            } else {
                metadata = objectStoreDriver.getDataAsString(metadataKey);
                simpleMetaObjectCache.put(metadataKey, metadata);
            }

            Map<String, String> blockLocationMap = null;
            try {
                blockLocationMap = objectMapper.readValue(metadata, Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (blockLocationMap.containsKey(blockId)) {
                String blockLocationString = blockLocationMap.get(blockId);
                try {
                    blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        blockLocation.setStorageLayerName(StorageLayerName.S3);
        blockLocation.setRange(true);

        return blockLocation;
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void batchPut(List<Block> blockList) {
        System.out.println("object num in each batch:  " + blockList.size());
        Map<String, String> keyValueMap = new HashMap<>();
        for (Block block : blockList) {
            if (block.getMetaDataString() == null) {
                putCount++;
                keyValueMap.put(block.getBlockId(), block.getDataString());
            } else {
                keyValueMap.put(block.getBlockId(), block.getDataString());
                keyValueMap.put(block.getBlockId()+".mapping", block.getMetaDataString());
                putCount+=2;
            }
        }
        objectStoreDriver.flushBatch(keyValueMap);
        keyValueMap.clear();
        logger.info("{} blocks have been put to S3", keyValueMap.size());
    }

    private static int TOTAL_S3_GET_REQUEST_NUM = 0;
    private static double TOTAL_S3_FETCH_SIZE = 0;
    @Override
    public List<Block> batchGet(List<String> blockIdList) {
        List<Block> resultBlockList = new ArrayList<>();
        List<BlockLocation> blockLocationList = new ArrayList<>();

        Map<String, List<BlockLocation>> uniqueObjectKeyMap = new HashMap<>(); // record the block number in each s3 object
        int count = 0;
        for (String blockId : blockIdList) {
            count++;
            BlockLocation blockLocation = getBlockLocation(blockId);
            //System.out.println("blockLocation count: " + count);
            if (blockLocation != null) {
                blockLocationList.add(blockLocation);
                if (uniqueObjectKeyMap.containsKey(blockLocation.getFilepath())) {
                    uniqueObjectKeyMap.get(blockLocation.getFilepath()).add(blockLocation);

                } else {
                    List<BlockLocation> list = new ArrayList<>();
                    list.add(blockLocation);
                    uniqueObjectKeyMap.put(blockLocation.getFilepath(), list);
                }
            }


        }
        //System.out.println("finish get block location");
        // first get unique data object
        List<Triple<String, Integer, Integer>> objectRequestKeyList = new ArrayList<>();
        for (String key : uniqueObjectKeyMap.keySet()) {
            Triple triple;
            List<BlockLocation> locs = uniqueObjectKeyMap.get(key);
            if (locs.size() >= 2) {

                List<BlockLocation> sortedLocs = new ArrayList<>(locs);
                sortedLocs.sort(Comparator.comparingInt(BlockLocation::getOffset));
                int newOffset = sortedLocs.get(0).getOffset();
                int endIndex = sortedLocs.get(sortedLocs.size()-1).getOffset() + sortedLocs.get(sortedLocs.size()-1).getLength();
                triple = Triple.of(key, newOffset, endIndex);
                objectRequestKeyList.add(triple);
                // calibrate the new offset in the location
                for (BlockLocation loc : locs) {
                    loc.setOffset(loc.getOffset()-newOffset);
                }

            } else {
                // only one data block is needed in this data object

                if (locs.size() == 1) {
                    BlockLocation location = locs.get(0);
                    if (location.isRange()) {
                        triple = Triple.of(key, location.getOffset(), location.getOffset()+location.getLength());
                        location.setOffset(0);
                    } else {
                        triple = Triple.of(key, -1, -1);
                    }
                    objectRequestKeyList.add(triple);


                }
            }

        }
        Map<String, String> objectDataMap = objectStoreDriver.getDataAsStringWithRangeBatch(objectRequestKeyList);
        System.out.println("Fetched S3 object number: " + objectRequestKeyList.size());
        for (String value : objectDataMap.values()) {
            System.out.println("fetched data length: " + value.length());
            TOTAL_S3_FETCH_SIZE += value.length() / 1024.0;
        }
        TOTAL_S3_GET_REQUEST_NUM += objectRequestKeyList.size();
        System.out.println("Total fetched S3 object num: " + TOTAL_S3_GET_REQUEST_NUM);
        System.out.println("Total fetched S3 data size: " + TOTAL_S3_FETCH_SIZE + " kb");
        for (int i = 0; i < blockLocationList.size(); i++ ) {
            BlockLocation location = blockLocationList.get(i);
            String dataString;
            //System.out.println("object data string length:  " + objectDataMap.get(location.getFilepath()).length());
            if (!location.isRange()) {
                dataString = objectDataMap.get(location.getFilepath());
                /*Block block = new Block();
                block.setDataString(dataString);
                block.setBlockId(blockIdList.get(i));
                resultBlockList.add(block);*/
            } else {

                dataString = objectDataMap.get(location.getFilepath());
                //System.out.println(dataString.length());
                //System.out.println(location);
                dataString = dataString.substring(location.getOffset(), location.getOffset() + location.getLength());



            }
            Block block = new Block();
            block.setDataString(dataString);
            block.setBlockId(blockIdList.get(i));
            resultBlockList.add(block);
        }
        objectDataMap.clear();
        objectDataMap = null;

        return resultBlockList;
    }

    /*public List<Block> batchGet(List<String> blockIdList) {
        List<Block> resultBlockList = new ArrayList<>();
        List<BlockLocation> blockLocationList = new ArrayList<>();

        Map<String, List<BlockLocation>> uniqueObjectKeyMap = new HashMap<>(); // record the block number in each s3 object
        int count = 0;
        for (String blockId : blockIdList) {
            count++;
            BlockLocation blockLocation = getBlockLocation(blockId);
            blockLocation.setBlockId(blockId);
            //System.out.println("blockLocation count: " + count);
            if (blockLocation != null) {
                blockLocationList.add(blockLocation);
                if (uniqueObjectKeyMap.containsKey(blockLocation.getFilepath())) {
                    uniqueObjectKeyMap.get(blockLocation.getFilepath()).add(blockLocation);

                } else {
                    List<BlockLocation> list = new ArrayList<>();
                    list.add(blockLocation);
                    uniqueObjectKeyMap.put(blockLocation.getFilepath(), list);
                }
            }
        }

        // do seperetion
        List<BlockLocation> sparseBlockLocationList = new ArrayList<>();
        List<BlockLocation> denseBlockLocationList = new ArrayList<>();
        for (String key : uniqueObjectKeyMap.keySet()) {
            List<BlockLocation> locs = uniqueObjectKeyMap.get(key);
            if (locs.size() >= 2) {
                List<BlockLocation> sortedLocs = new ArrayList<>(locs);
                sortedLocs.sort(Comparator.comparingInt(BlockLocation::getOffset));

                if (locs.size() == 2) {
                    BlockLocation prev = sortedLocs.get(0);
                    BlockLocation next = sortedLocs.get(1);
                    if (next.getOffset() - prev.getOffset() > 1024*1024) {
                        // there is a huge gap between two block, so fetch previous block using the seperate request
                        sparseBlockLocationList.addAll(locs);
                    } else {
                        denseBlockLocationList.addAll(locs);
                    }
                } else {
                    // we check if these is a huge gap difference
                    int minGap = Integer.MAX_VALUE;
                    int maxGap = 0;
                    for (int i = 0; i < locs.size() - 1; i++) {
                        int prev = sortedLocs.get(i).getOffset();
                        int next = sortedLocs.get(i + 1).getOffset();
                        int gap = next - prev;
                        if (gap > maxGap) {
                            maxGap = gap;
                        }
                        if (gap < minGap) {
                            minGap = gap;
                        }
                    }
                    if (maxGap - minGap > 1024 * 1024 && locs.size() < 9) {
                        sparseBlockLocationList.addAll(locs);
                    } else {
                        denseBlockLocationList.addAll(locs);
                    }
                }

            } else {
                sparseBlockLocationList.addAll(locs);
            }

        }

        System.out.println("sparse block num: " + sparseBlockLocationList.size());
        System.out.println("dense block num: " + denseBlockLocationList.size());
        resultBlockList.addAll(batchGetForSparse(sparseBlockLocationList));
        resultBlockList.addAll(batchGetForDense(denseBlockLocationList));

        return resultBlockList;
    }*/

    /**
     * if only few data blocks are in the same object, then use this function -> send a request for each block
     * @param locationList
     * @return
     */
    /*public List<Block> batchGetForSparse(List<BlockLocation> locationList) {
        List<Block> resultBlockList = new ArrayList<>();

        List<Triple<String, Integer, Integer>> objectRequestKeyList = new ArrayList<>();
        for (BlockLocation location : locationList) {
            Triple triple = Triple.of(location.getFilepath(), location.getOffset(), location.getOffset()+location.getLength());
            objectRequestKeyList.add(triple);
        }
        List<String> dataBlockStringList = objectStoreDriver.getDataAsStringWithRangeWithDuplicatedKeyBatch(objectRequestKeyList);
        System.out.println("Fetched S3 object number (sparse): " + objectRequestKeyList.size());
        TOTAL_S3_GET_REQUEST_NUM += objectRequestKeyList.size();
        System.out.println("Total fetched S3 object num: " + TOTAL_S3_GET_REQUEST_NUM);
        int i = 0;
        for (String dataString : dataBlockStringList) {
            Block block = new Block();
            block.setDataString(dataString);
            block.setBlockId(locationList.get(i).getBlockId());
            resultBlockList.add(block);
            i++;
        }

        return resultBlockList;
    }*/

    /**
     * if most data blocks are in the same object, then use this function -> range fetch data in a object
     * @param blockLocationList
     * @return
     */
    /*public List<Block> batchGetForDense(List<BlockLocation> blockLocationList) {
        List<Block> resultBlockList = new ArrayList<>();


        Map<String, List<BlockLocation>> uniqueObjectKeyMap = new HashMap<>(); // record the block number in each s3 object
        int count = 0;
        for (BlockLocation blockLocation : blockLocationList) {
           count++;

            //System.out.println("blockLocation count: " + count);
            if (blockLocation != null) {

                if (uniqueObjectKeyMap.containsKey(blockLocation.getFilepath())) {
                    uniqueObjectKeyMap.get(blockLocation.getFilepath()).add(blockLocation);

                } else {
                    List<BlockLocation> list = new ArrayList<>();
                    list.add(blockLocation);
                    uniqueObjectKeyMap.put(blockLocation.getFilepath(), list);
                }
            }


        }
        //System.out.println("finish get block location");
        // first get unique data object
        List<Triple<String, Integer, Integer>> objectRequestKeyList = new ArrayList<>();
        for (String key : uniqueObjectKeyMap.keySet()) {
            Triple triple;
            List<BlockLocation> locs = uniqueObjectKeyMap.get(key);
            if (locs.size() >= 2) {

                List<BlockLocation> sortedLocs = new ArrayList<>(locs);
                sortedLocs.sort(Comparator.comparingInt(BlockLocation::getOffset));
                int newOffset = sortedLocs.get(0).getOffset();
                int endIndex = sortedLocs.get(sortedLocs.size()-1).getOffset() + sortedLocs.get(sortedLocs.size()-1).getLength();
                triple = Triple.of(key, newOffset, endIndex);
                objectRequestKeyList.add(triple);
                // calibrate the new offset in the location
                for (BlockLocation loc : locs) {
                    loc.setOffset(loc.getOffset()-newOffset);
                }

            } else {
                // only one data block is needed in this data object

                if (locs.size() == 1) {
                    BlockLocation location = locs.get(0);
                    if (location.isRange()) {
                        triple = Triple.of(key, location.getOffset(), location.getOffset()+location.getLength());
                        location.setOffset(0);
                    } else {
                        triple = Triple.of(key, -1, -1);
                    }
                    objectRequestKeyList.add(triple);


                }
            }

        }
        Map<String, String> objectDataMap = objectStoreDriver.getDataAsStringWithRangeBatch(objectRequestKeyList);
        System.out.println("Fetched S3 object number (dense): " + objectRequestKeyList.size());
        TOTAL_S3_GET_REQUEST_NUM += objectRequestKeyList.size();
        System.out.println("Total fetched S3 object num: " + TOTAL_S3_GET_REQUEST_NUM);
        for (int i = 0; i < blockLocationList.size(); i++ ) {
            BlockLocation location = blockLocationList.get(i);
            String dataString;
            if (!location.isRange()) {
                dataString = objectDataMap.get(location.getFilepath());
                *//*Block block = new Block();
                block.setDataString(dataString);
                block.setBlockId(blockIdList.get(i));
                resultBlockList.add(block);*//*
            } else {

                dataString = objectDataMap.get(location.getFilepath());
                //System.out.println(dataString.length());
                //System.out.println(location);
                dataString = dataString.substring(location.getOffset(), location.getOffset() + location.getLength());



            }
            Block block = new Block();
            block.setDataString(dataString);
            block.setBlockId(location.getBlockId());
            resultBlockList.add(block);
        }
        objectDataMap.clear();
        objectDataMap = null;

        return resultBlockList;
    }*/


    public List<Block> batchGetWithoutOptimization(List<String> blockIdList) {
        List<Block> resultBlockList = new ArrayList<>();
        List<BlockLocation> blockLocationList = new ArrayList<>();

        Map<String, List<BlockLocation>> uniqueObjectKeyMap = new HashMap<>(); // record the block number in each s3 object
        int count = 0;
        for (String blockId : blockIdList) {
            count++;
            BlockLocation blockLocation = getBlockLocation(blockId);
            //System.out.println("blockLocation count: " + count);
            if (blockLocation != null) {
                blockLocationList.add(blockLocation);
                if (uniqueObjectKeyMap.containsKey(blockLocation.getFilepath())) {
                    uniqueObjectKeyMap.get(blockLocation.getFilepath()).add(blockLocation);

                } else {
                    List<BlockLocation> list = new ArrayList<>();
                    list.add(blockLocation);
                    uniqueObjectKeyMap.put(blockLocation.getFilepath(), list);
                }
            }


        }
        //System.out.println("finish get block location");
        // first get unique data object
        List<Triple<String, Integer, Integer>> objectRequestKeyList = new ArrayList<>();
        for (String key : uniqueObjectKeyMap.keySet()) {
            Triple triple;
            List<BlockLocation> locs = uniqueObjectKeyMap.get(key);
            if (uniqueObjectKeyMap.get(key).size() >= 2) {
                triple = Triple.of(key, -1, -1);
                objectRequestKeyList.add(triple);
            } else {
                // only one data block is needed in this data object
                if (locs.size() == 1) {
                    BlockLocation location = locs.get(0);
                    if (location.isRange()) {
                        triple = Triple.of(key, location.getOffset(), location.getOffset()+location.getLength());
                        location.setOffset(0);
                    } else {
                        triple = Triple.of(key, -1, -1);
                    }
                    objectRequestKeyList.add(triple);
                }
            }

        }
        Map<String, String> objectDataMap = objectStoreDriver.getDataAsStringWithRangeBatch(objectRequestKeyList);
        System.out.println("Fetched S3 object number: " + objectRequestKeyList.size());
        TOTAL_S3_GET_REQUEST_NUM += objectRequestKeyList.size();
        System.out.println("Total fetched S3 object num: " + TOTAL_S3_GET_REQUEST_NUM);
        for (int i = 0; i < blockLocationList.size(); i++ ) {
            BlockLocation location = blockLocationList.get(i);
            String dataString;
            if (!location.isRange()) {
                dataString = objectDataMap.get(location.getFilepath());
                /*Block block = new Block();
                block.setDataString(dataString);
                block.setBlockId(blockIdList.get(i));
                resultBlockList.add(block);*/
            } else {
                dataString = objectDataMap.get(location.getFilepath());
                dataString = dataString.substring(location.getOffset(), location.getOffset() + location.getLength());

            }
            Block block = new Block();
            block.setDataString(dataString);
            block.setBlockId(blockIdList.get(i));
            resultBlockList.add(block);
        }
        objectDataMap.clear();
        objectDataMap = null;

        return resultBlockList;
    }


    // backup
    /*public List<Block> batchGet(List<String> blockIdList) {
        List<Block> resultBlockList = new ArrayList<>();
        List<BlockLocation> blockLocationList = new ArrayList<>();

        Map<String, List<BlockLocation>> uniqueObjectKeyMap = new HashMap<>(); // record the block number in each s3 object
        int count = 0;
        for (String blockId : blockIdList) {
            count++;
            BlockLocation blockLocation = getBlockLocation(blockId);
            //System.out.println("blockLocation count: " + count);
            if (blockLocation != null) {
                blockLocationList.add(blockLocation);
                if (uniqueObjectKeyMap.containsKey(blockLocation.getFilepath())) {
                    uniqueObjectKeyMap.get(blockLocation.getFilepath()).add(blockLocation);

                } else {
                    List<BlockLocation> list = new ArrayList<>();
                    list.add(blockLocation);
                    uniqueObjectKeyMap.put(blockLocation.getFilepath(), list);
                }
            }


        }
        //System.out.println("finish get block location");
        // first get unique data object
        List<Triple<String, Integer, Integer>> objectRequestKeyList = new ArrayList<>();
        for (String key : uniqueObjectKeyMap.keySet()) {
            Triple triple;
            List<BlockLocation> locs = uniqueObjectKeyMap.get(key);
            if (locs.size() >= 2) {

                List<BlockLocation> sortedLocs = new ArrayList<>(locs);
                sortedLocs.sort(Comparator.comparingInt(BlockLocation::getOffset));
                int newOffset = sortedLocs.get(0).getOffset();
                int endIndex = sortedLocs.get(sortedLocs.size()-1).getOffset() + sortedLocs.get(sortedLocs.size()-1).getLength();
                triple = Triple.of(key, newOffset, endIndex);
                objectRequestKeyList.add(triple);
                // calibrate the new offset in the location
                for (BlockLocation loc : locs) {
                    loc.setOffset(loc.getOffset()-newOffset);
                }

            } else {
                // only one data block is needed in this data object

                if (locs.size() == 1) {
                    BlockLocation location = locs.get(0);
                    if (location.isRange()) {
                        triple = Triple.of(key, location.getOffset(), location.getOffset()+location.getLength());
                        location.setOffset(0);
                    } else {
                        triple = Triple.of(key, -1, -1);
                    }
                    objectRequestKeyList.add(triple);


                }
            }

        }
        Map<String, String> objectDataMap = objectStoreDriver.getDataAsStringWithRangeBatch(objectRequestKeyList);
        System.out.println("Fetched S3 object number: " + objectRequestKeyList.size());
        TOTAL_S3_GET_REQUEST_NUM += objectRequestKeyList.size();
        System.out.println("Total fetched S3 object num: " + TOTAL_S3_GET_REQUEST_NUM);
        for (int i = 0; i < blockLocationList.size(); i++ ) {
            BlockLocation location = blockLocationList.get(i);
            String dataString;
            if (!location.isRange()) {
                dataString = objectDataMap.get(location.getFilepath());
                *//*Block block = new Block();
                block.setDataString(dataString);
                block.setBlockId(blockIdList.get(i));
                resultBlockList.add(block);*//*
            } else {

                dataString = objectDataMap.get(location.getFilepath());
                //System.out.println(dataString.length());
                //System.out.println(location);
                dataString = dataString.substring(location.getOffset(), location.getOffset() + location.getLength());



            }
            Block block = new Block();
            block.setDataString(dataString);
            block.setBlockId(blockIdList.get(i));
            resultBlockList.add(block);
        }
        objectDataMap.clear();
        objectDataMap = null;

        return resultBlockList;
    }*/


    @Override
    public void clearAll() {
        // remove data
        objectStoreDriver.deleteBucket(bucketName);
        logger.info("clear blocks in S3");

    }

    @Override
    public void clear(List<String> blockIdList) {
        // remove data
        for (String blockId : blockIdList) {
            objectStoreDriver.remove(blockId);
        }
        logger.info("clear blocks in S3: [{}]", blockIdList);

    }

    @Override
    public void close() {
        objectStoreDriver.close();
        simpleMetaObjectCache.clear();
    }
}
