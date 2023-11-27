package com.rogerguo.test.store;

import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.NodeTuple;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.TrajectorySegmentMeta;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.storage.Block;
import com.rogerguo.test.storage.StorageConfiguration;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.TieredCloudStorageManager;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.layer.StorageLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author yangguo
 * @create 2021-10-04 4:12 PM
 **/
public class SeriesStore {

    private int maxChunkSize;

    private HashMap<String, SingleSeries> seriesStore;

    private ChunkIdManager chunkIdManager;

    private TieredCloudStorageManager tieredCloudStorageManager;

    private SpatialTemporalTree indexForImmutableChunks;

    private HeadChunkIndex indexForHeadChunks;

    private boolean enableCache = StoreConfig.ENABLE_STORE_CACHE;

    private int chunkCacheSize = 150000;

    private int initCapacity = 1 + (int) (chunkCacheSize / 0.75);

    private Map<String, Chunk> simpleChunkCache = new HashMap<>(initCapacity); // key is sid + chunkId

    private Queue<String> chunkCacheQueue = new LinkedList<>();  // used to kick out data object

    //private StoreCache storeCache = new StoreCache(chunkCacheSize);

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * used to create a series store with several options
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     * @param headChunkIndex
     * @param dimensionNormalizer
     * @param s3LayoutSchema
     * @param timestampModeForIdManager
     */
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks, HeadChunkIndex headChunkIndex, DimensionNormalizer dimensionNormalizer, S3LayoutSchema s3LayoutSchema, String timestampModeForIdManager) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager(dimensionNormalizer, s3LayoutSchema, timestampModeForIdManager);
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = headChunkIndex;
    }



    /**
     * used for a brand-new store with custom normalizer
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks, DimensionNormalizer dimensionNormalizer, HeadChunkIndex headChunkIndex) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager(dimensionNormalizer);
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = headChunkIndex;
    }

    /**
     * used for a brand-new store with custom normalizer and layout schema
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks, HeadChunkIndex headChunkIndex, DimensionNormalizer dimensionNormalizer, S3LayoutSchema s3LayoutSchema) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager(dimensionNormalizer, s3LayoutSchema);
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = headChunkIndex;
    }


    /**
     * used for a brand-new store
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager();
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
    }

    /**
     * used for existed stores that already have indexes
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexConfiguration
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, IndexConfiguration indexConfiguration) {
        SpatialTemporalTree rebuildIndex = new SpatialTemporalTree(indexConfiguration).loadAndRebuildIndex();
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager();
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = rebuildIndex;
    }

    @Deprecated
    public static SeriesStore initNewStoreWithOptimizedS3FlushForTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructureWithOptimizedS3Flush(storageConfiguration);

        IndexConfiguration indexConfiguration = SpatialTemporalTree.getDefaultIndexConfiguration();
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration);

        int maxChunkSize = 1000;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    @Deprecated
    public static SeriesStore initNewStoreForTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructure(storageConfiguration);

        IndexConfiguration indexConfiguration = SpatialTemporalTree.getDefaultIndexConfiguration();
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration);

        int maxChunkSize = 1000;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    @Deprecated
    public static SeriesStore initNewStoreForInMemTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructureForInMemTest(storageConfiguration);

        SpatialTemporalTree spatialTemporalTree = null;

        int maxChunkSize = Integer.MAX_VALUE;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    @Deprecated
    public static SeriesStore initExistedStoreForTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructure(storageConfiguration);

        IndexConfiguration indexConfiguration = SpatialTemporalTree.getDefaultIndexConfiguration();
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration).loadAndRebuildIndex();

        int maxChunkSize = 1000;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    public boolean appendSeriesPoint(TrajectoryPoint point) {

        String sid = point.getOid();

        if (seriesStore.containsKey(sid)) {
            seriesStore.get(sid).appendPoint(point);
        } else {
            // comes a new trajectory series
            SingleSeries series = new SingleSeries(sid, maxChunkSize, tieredCloudStorageManager, chunkIdManager, indexForImmutableChunks, indexForHeadChunks);
            series.appendPoint(point);
            seriesStore.put(sid, series);

        }

        return true;
    }

    public List<Chunk> idTemporalQuery(String oid, long startTime, long stopTime) {
        logger.info("run id temporal query: oid = " + oid + ", startTime = " + startTime + ", stopTime = " + stopTime);
        List<Chunk> resultChunks = new ArrayList<>();

        // 1. check in-memory active part
        if (seriesStore.containsKey(oid)) {
            Chunk headChunk = seriesStore.get(oid).getHeadChunk();
            if (headChunk.getChunk().get(0).getTimestamp() <= stopTime) {
                resultChunks.add(headChunk);
            }
        }

        // 2. check disk/immutable part
        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(startTime, stopTime, oid);
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForIdTemporal(predicate);
        logger.info("index entry from immutable chunk index: " + resultTupleList.size());
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            //Chunk chunk = Chunk.deserialize(tieredCloudStorageManager.get(blockId).getDataString());
            Chunk chunk = null;
            if (enableCache) {
                chunk = checkChunkCache(blockId);
            } else {
                chunk = Chunk.deserialize(tieredCloudStorageManager.get(blockId).getDataString());
            }
            resultChunks.add(chunk);
        }

        return resultChunks;
    }

    public List<Chunk> idTemporalQueryBatch(String oid, long startTime, long stopTime) {
        logger.info("run id temporal query: oid = " + oid + ", startTime = " + startTime + ", stopTime = " + stopTime);
        List<Chunk> resultChunks = new ArrayList<>();
        long totalStart = System.currentTimeMillis();

        long indexTime = 0;
        // 1. check in-memory active part
        if (seriesStore.containsKey(oid)) {
            Chunk headChunk = seriesStore.get(oid).getHeadChunk();
            if (headChunk.getChunk().get(0).getTimestamp() <= stopTime) {
                resultChunks.add(headChunk);
            }
        }

        // 2. check disk/immutable part
        long begin = System.currentTimeMillis();
        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(startTime, stopTime, oid);
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForIdTemporal(predicate);
        long stop = System.currentTimeMillis();
        indexTime = indexTime + stop - begin;
        logger.info("index entry from immutable chunk index: " + resultTupleList.size());
        List<String> blockIdList = new ArrayList<>();
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            if (enableCache && simpleChunkCache.containsKey(blockId)) {
                cacheHitCount++;
                resultChunks.add(simpleChunkCache.get(blockId));
            } else {
                blockIdList.add(blockId);
            }
        }

        long blockGetTime = 0;
        begin = System.currentTimeMillis();
        List<Block> blockList = tieredCloudStorageManager.batchGet(blockIdList);
        stop = System.currentTimeMillis();
        blockGetTime = stop - begin;

        long blockDeserializationTime;
        begin = System.currentTimeMillis();
        for (Block block : blockList) {
            Chunk chunk = Chunk.deserialize(block.getDataString());
            resultChunks.add(chunk);

            if (enableCache) {
                cacheMissCount++;
                if (simpleChunkCache.keySet().size() > chunkCacheSize) {
                    String kickedKey = chunkCacheQueue.poll();
                    simpleChunkCache.remove(kickedKey);
                    kickCount++;
                }


                if (random.nextInt() % 3 == 1) {
                    simpleChunkCache.put(block.getBlockId(), chunk);
                    chunkCacheQueue.offer(block.getBlockId());
                }
            }

        }
        stop = System.currentTimeMillis();
        blockDeserializationTime = stop - begin;
        long totalStop = System.currentTimeMillis();

        System.out.println("index lookup time: " + indexTime);
        System.out.println("block get time:  " + blockGetTime);
        System.out.println("block deserialization time: " + blockDeserializationTime);
        System.out.println("total time: " + (totalStop - totalStart));

        return resultChunks;
    }




    public List<TrajectoryPoint> idTemporalQueryWithRefinement(String oid, long startTime, long stopTime) {
        List<Chunk> chunkList;
        if (StoreConfig.ENABLE_BATCH_GET_OPERATION) {
            chunkList = idTemporalQueryBatch(oid, startTime, stopTime);
        } else {
            chunkList = idTemporalQuery(oid, startTime, stopTime);
        }
        return refineIdTemporalQuery(chunkList, new IdTemporalQueryPredicate(startTime, stopTime, oid));
    }

    public List<Chunk> spatialTemporalRangeQuery(long startTime, long stopTime, SpatialBoundingBox spatialBoundingBox) {
        logger.info("run spatio-temporal range query: startTime = " + startTime + ", stopTime = " + stopTime + ", boundingBox = " + spatialBoundingBox);

        List<Chunk> resultChunks = new ArrayList<>();

        long indexTime = 0;
        // 1. check in-memory active part
        long begin = System.currentTimeMillis();
        Set<String> sidSet = indexForHeadChunks.searchForSpatial(spatialBoundingBox);
        long stop = System.currentTimeMillis();
        indexTime = stop - begin;
        logger.info("index entry from head chunk index: " + sidSet.size());
        if (sidSet!= null && !sidSet.isEmpty()) {
            for (String sid : sidSet) {
                if (seriesStore.get(sid).getHeadChunk().getChunk().get(0).getTimestamp() <= stopTime) {
                    resultChunks.add(seriesStore.get(sid).getHeadChunk());
                }
            }
        }

        // 2. check disk/immutable part
        begin = System.currentTimeMillis();
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForSpatialTemporal(new SpatialTemporalRangeQueryPredicate(startTime, stopTime, spatialBoundingBox.getLowerLeft(), spatialBoundingBox.getUpperRight()));
        stop = System.currentTimeMillis();
        indexTime = indexTime + stop - begin;
        logger.info("index entry from immutable chunk index: " + resultTupleList.size());
        System.out.println("index entry from immutable chunk index: " + resultTupleList.size());
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            Chunk chunk = null;
            if (enableCache) {
                chunk = checkChunkCache(blockId);
            } else {
                chunk = Chunk.deserialize(tieredCloudStorageManager.get(blockId).getDataString());
            }
            resultChunks.add(chunk);
        }
        System.out.println("index lookup time: " + indexTime);

        /*long timeRangeTotal = 0;
        double longitudeRangeTotal = 0;
        double latitudeRangeTotal = 0;
        for (Chunk chunk : resultChunks) {
            longitudeRangeTotal += chunk.getMeta().getUpperRight().getLongitude() - chunk.getMeta().getLowerLeft().getLongitude();
            latitudeRangeTotal += chunk.getMeta().getUpperRight().getLatitude() - chunk.getMeta().getLowerLeft().getLatitude();
            timeRangeTotal += chunk.getMeta().getStopTimestamp() - chunk.getMeta().getStartTimestamp();
        }
        System.out.println("average time range: " + (timeRangeTotal / resultChunks.size()));
        System.out.println("average longitude range: " + (longitudeRangeTotal / resultChunks.size()));
        System.out.println("average latitude range: " + (latitudeRangeTotal / resultChunks.size()));*/
        return resultChunks;
    }

    public List<Chunk> spatialTemporalRangeQueryBatch(long startTime, long stopTime, SpatialBoundingBox spatialBoundingBox) {
        logger.info("run spatio-temporal range query: startTime = " + startTime + ", stopTime = " + stopTime + ", boundingBox = " + spatialBoundingBox);

        List<Chunk> resultChunks = new ArrayList<>();
        long totalStart = System.currentTimeMillis();

        long indexTime = 0;
        // 1. check in-memory active part
        long begin = System.currentTimeMillis();
        Set<String> sidSet = indexForHeadChunks.searchForSpatial(spatialBoundingBox);
        long stop = System.currentTimeMillis();
        indexTime = stop - begin;
        logger.info("index entry from head chunk index: " + sidSet.size());
        if (sidSet!= null && !sidSet.isEmpty()) {
            for (String sid : sidSet) {
                if (seriesStore.get(sid).getHeadChunk().getChunk().get(0).getTimestamp() <= stopTime) {
                    resultChunks.add(seriesStore.get(sid).getHeadChunk());
                }
            }
        }

        // 2. check disk/immutable part
        begin = System.currentTimeMillis();
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForSpatialTemporal(new SpatialTemporalRangeQueryPredicate(startTime, stopTime, spatialBoundingBox.getLowerLeft(), spatialBoundingBox.getUpperRight()));
        stop = System.currentTimeMillis();
        indexTime = indexTime + stop - begin;
        logger.info("index entry from immutable chunk index: " + resultTupleList.size());
        System.out.println("index entry from immutable chunk index: " + resultTupleList.size());
        List<String> blockIdList = new ArrayList<>();
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();

            if (enableCache && simpleChunkCache.containsKey(blockId)) {
                cacheHitCount++;
                resultChunks.add(simpleChunkCache.get(blockId));
            } else {
                blockIdList.add(blockId);
            }
        }

        long blockGetTime;
        begin = System.currentTimeMillis();
        List<Block> blockList = tieredCloudStorageManager.batchGet(blockIdList);
        stop = System.currentTimeMillis();
        blockGetTime = stop - begin;

        long blockDeserializationTime;
        begin = System.currentTimeMillis();
        for (Block block : blockList) {
            Chunk chunk = Chunk.deserialize(block.getDataString());
            resultChunks.add(chunk);
            if (enableCache) {
                cacheMissCount++;
                if (simpleChunkCache.keySet().size() > chunkCacheSize) {
                    String kickedKey = chunkCacheQueue.poll();
                    simpleChunkCache.remove(kickedKey);
                    kickCount++;
                }


                if (random.nextInt() % 3 == 1) {
                    simpleChunkCache.put(block.getBlockId(), chunk);
                    chunkCacheQueue.offer(block.getBlockId());
                }
            }
        }
        stop = System.currentTimeMillis();
        blockDeserializationTime = stop - begin;

        long totalStop = System.currentTimeMillis();
        System.out.println("index lookup time: " + indexTime);
        System.out.println("block get time:  " + blockGetTime);
        System.out.println("block deserialization time: " + blockDeserializationTime);
        System.out.println("total time: " + (totalStop - totalStart));

        /*long timeRangeTotal = 0;
        double longitudeRangeTotal = 0;
        double latitudeRangeTotal = 0;
        for (Chunk chunk : resultChunks) {
            longitudeRangeTotal += chunk.getMeta().getUpperRight().getLongitude() - chunk.getMeta().getLowerLeft().getLongitude();
            latitudeRangeTotal += chunk.getMeta().getUpperRight().getLatitude() - chunk.getMeta().getLowerLeft().getLatitude();
            timeRangeTotal += chunk.getMeta().getStopTimestamp() - chunk.getMeta().getStartTimestamp();
        }
        if (resultChunks != null && resultChunks.size() > 0) {
            System.out.println("average time range: " + (timeRangeTotal / resultChunks.size()));
            System.out.println("average longitude range: " + (longitudeRangeTotal / resultChunks.size()));
            System.out.println("average latitude range: " + (latitudeRangeTotal / resultChunks.size()));

        }*/
        return resultChunks;
    }

    static int cacheHitCount = 0;
    static int cacheMissCount = 0;
    static int kickCount = 0;
    static Random random = new Random(2);
    public Chunk checkChunkCache(String key) {
        //System.out.println("cache hit count: " + cacheHitCount + "; cache miss count: " + cacheMissCount + "; kick count: " + kickCount);
        if (simpleChunkCache.containsKey(key)) {
            cacheHitCount++;
            return simpleChunkCache.get(key);
        } else {
            cacheMissCount++;
            Chunk chunk = Chunk.deserialize(tieredCloudStorageManager.get(key).getDataString());
            if (simpleChunkCache.keySet().size() > chunkCacheSize) {
                String kickedKey = chunkCacheQueue.poll();
                simpleChunkCache.remove(kickedKey);
                kickCount++;
            }


            if (random.nextInt() % 3 == 1) {
                simpleChunkCache.put(key, chunk);
                chunkCacheQueue.offer(key);
            }
            return chunk;
        }


    }

    public List<TrajectoryPoint> spatialTemporalRangeQueryWithRefinement(long startTime, long stopTime, SpatialBoundingBox spatialBoundingBox) {
        List<Chunk> chunkList;
        if (StoreConfig.ENABLE_BATCH_GET_OPERATION) {
            chunkList = spatialTemporalRangeQueryBatch(startTime, stopTime, spatialBoundingBox);
        } else {
            chunkList = spatialTemporalRangeQuery(startTime, stopTime, spatialBoundingBox);
        }
        return refineSpatioTemporalQuery(chunkList, new SpatialTemporalRangeQueryPredicate(startTime, stopTime, spatialBoundingBox.getLowerLeft(), spatialBoundingBox.getUpperRight()));
    }

    @Deprecated
    public Set<String> refine(Set<String> sidSet, SpatialBoundingBox boundingBox) {
        Set<String> refinedIdSet = new HashSet<>();

        for (String sid : sidSet) {
            Chunk chunk = seriesStore.get(sid).getHeadChunk();
            boolean mark = false;
            for (TrajectoryPoint point : chunk.getChunk()) {
                if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                    mark = true;
                    break;
                }
            }
            if (mark) {
                refinedIdSet.add(sid);
            }

        }
        return refinedIdSet;
    }

    public List<TrajectoryPoint> refineReturnPoints(Set<String> sidSet, SpatialBoundingBox boundingBox) {
        List<TrajectoryPoint> resultPointList = new ArrayList<>();
        for (String sid : sidSet) {
            Chunk chunk = seriesStore.get(sid).getHeadChunk();
            boolean mark = false;
            for (TrajectoryPoint point : chunk.getChunk()) {
                if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                    resultPointList.add(point);
                }
            }
        }

        return resultPointList;
    }

    public List<TrajectoryPoint> refineIdTemporalQuery(List<Chunk> chunkList, IdTemporalQueryPredicate predicate) {
        long startTime = System.currentTimeMillis();

        List<TrajectoryPoint> pointList = new ArrayList<>();
        int containedCount = 0;
        int countWithoutRefinement = 0;
        for (Chunk chunk : chunkList) {
            countWithoutRefinement = countWithoutRefinement + chunk.getChunk().size();
            if (chunk.getMeta() != null  && predicate.getStopTimestamp() >= chunk.getMeta().getStopTimestamp()
            && predicate.getStartTimestamp() <= chunk.getMeta().getStartTimestamp()) {
                pointList.addAll(chunk.getChunk());
                containedCount++;
            } else {

                for (TrajectoryPoint point : chunk.getChunk()) {
                    if (predicate.getDeviceId().equals(point.getOid()) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                        pointList.add(point);
                    }
                }
            }
        }
        logger.info("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());

        long stopTime = System.currentTimeMillis();
        System.out.println("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());
        System.out.println("total chunk num: " + chunkList.size() + ", contained chunk num: " + containedCount);
        System.out.println("refine time:  " + (stopTime - startTime));
        return pointList;
    }

    public List<TrajectoryPoint> refineSpatioTemporalQuery(List<Chunk> chunkList, SpatialTemporalRangeQueryPredicate predicate) {
        long startTime = System.currentTimeMillis();
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int containedCount = 0;
        int countWithoutRefinement = 0;
        SpatialBoundingBox boundingBoxPredicate = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        for (Chunk chunk : chunkList) {
            //System.out.println(chunk.getMeta());
            countWithoutRefinement = countWithoutRefinement + chunk.getChunk().size();
            if (chunk.getMeta() != null && SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBoxPredicate, chunk.getMeta().getLowerLeft())
                    && SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBoxPredicate, chunk.getMeta().getUpperRight())
                    && predicate.getStopTimestamp() >= chunk.getMeta().getStopTimestamp()
                    && predicate.getStartTimestamp() <= chunk.getMeta().getStartTimestamp()) {
                pointList.addAll(chunk.getChunk());
                containedCount++;

            } else {

                for (TrajectoryPoint point : chunk.getChunk()) {
                    if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBoxPredicate, point) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                        pointList.add(point);
                    }
                }
            }
        }
        long stopTime = System.currentTimeMillis();
        logger.info("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());
        System.out.println("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());
        System.out.println("total chunk num: " + chunkList.size() + ", contained chunk num: " + containedCount);
        System.out.println("refine time:  " + (stopTime - startTime));
        return pointList;
    }

    private void flush() {

        // 1. put active head chunk to storage
        Set<String> seriesKeys = seriesStore.keySet();
        for (String seriesKey : seriesKeys) {
            SingleSeries series = seriesStore.get(seriesKey);
            Chunk headChunk = series.getHeadChunk();
            headChunk.setChunkId(chunkIdManager.getChunkIdWithTags(headChunk.getSid(), headChunk.getChunk()));
            Block block = new Block(ChunkIdManager.generateStringBlockId(headChunk), Chunk.serialize(headChunk));
            tieredCloudStorageManager.put(block);
            TrajectorySegmentMeta indexEntry = SingleSeries.generateIndexEntry(headChunk);
            indexForImmutableChunks.insert(indexEntry);
        }

    }


    public void stop() {
        // 1. flush all memory data
        flush();

        // 2. close storage layers and index
        indexForImmutableChunks.close();
        tieredCloudStorageManager.close();

    }

    public void flushDataToS3() {
        // 1. flush all memory data
        flush();

        // 2. flush to s3
        for (StorageLayerName storageLayerName : tieredCloudStorageManager.getStorageLayerHierarchyNameList()) {
            StorageLayer storageLayer = tieredCloudStorageManager.getStorageLayerMap().get(storageLayerName);
            storageLayer.flush();
        }
    }

    public void flushDataToDisk() {
        // 1. flush all memory data
        flush();

        // 1. flush to disk
        StorageLayer storageLayer = tieredCloudStorageManager.getStorageLayerMap().get(StorageLayerName.IMMUTABLEMEM);
        storageLayer.flush();

        indexForImmutableChunks.close();
    }

    public SpatialTemporalTree getIndexForImmutableChunks() {
        return indexForImmutableChunks;
    }


    public HashMap<String, SingleSeries> getSeriesStore() {
        return seriesStore;
    }

    public void setChunkIdManager(ChunkIdManager chunkIdManager) {
        this.chunkIdManager = chunkIdManager;
    }
}
