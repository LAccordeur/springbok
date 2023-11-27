package com.rogerguo.test.store;

import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.NodeTuple;
import com.rogerguo.test.index.SpatialTemporalTree;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.storage.driver.PersistenceDriver;

import java.io.File;
import java.util.*;

/**
 * @Description
 * @Date 2021/4/25 10:46
 * @Created by GUO Yang
 */
@Deprecated
public class SeriesMemoryStore {

    private int maxChunkSize;

    private int immutableMemorySize;

    private HashMap<String, SingleSeries> seriesStore;

    private ImmutableMemoryRegion immutableMemoryRegion;

    private ChunkIdManager chunkIdManager;

    private PersistenceDriver persistenceDriver;

    private SpatialTemporalTree indexForImmutableChunks;

    private HeadChunkIndexWithTwoLevelGrid indexForHeadChunks = new HeadChunkIndexWithTwoLevelGrid();

    public SeriesMemoryStore(int maxChunkSize, int immutableMemorySize, ChunkIdManager chunkIdManager, PersistenceDriver persistenceDriver, SpatialTemporalTree indexForImmutableChunks) {
        this.maxChunkSize = maxChunkSize;
        this.immutableMemorySize = immutableMemorySize;
        this.persistenceDriver = persistenceDriver;
        this.chunkIdManager = chunkIdManager;
        this.immutableMemoryRegion = new ImmutableMemoryRegion(persistenceDriver);
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
    }

    public boolean appendSeriesPoint(TrajectoryPoint point) {

        String sid = point.getOid();

        if (seriesStore.containsKey(sid)) {
            seriesStore.get(sid).appendPoint(point);
        } else {
            // comes a new trajectory series
            SingleSeries series = new SingleSeries(sid, maxChunkSize, null, chunkIdManager, indexForImmutableChunks, indexForHeadChunks);
            series.appendPoint(point);
            seriesStore.put(sid, series);

        }

        return true;
    }

    public List<Chunk> idTemporalQuery(String oid, long startTime, long stopTime) {
        List<Chunk> resultChunks = new ArrayList<>();

        // 1. check in-memory active part
        Chunk headChunk = seriesStore.get(oid).getHeadChunk();
        if (headChunk.getChunk().get(0).getTimestamp() <= stopTime) {
            resultChunks.add(headChunk);
        }

        // 2. check disk/immutable part
        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(startTime, stopTime, oid);
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForIdTemporal(predicate);
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            String[] items = blockId.split("\\.");
            Chunk chunk = immutableMemoryRegion.getImmutableMemory().get(items[0]).get(items[1]);
            resultChunks.add(chunk);
        }

        return resultChunks;
    }

    public List<Chunk> spatialTemporalRangeQuery(long startTime, long stopTime, SpatialBoundingBox spatialBoundingBox) {
        List<Chunk> resultChunks = new ArrayList<>();

        // 1. check in-memory active part
        Set<String> sidSet = indexForHeadChunks.searchForSpatial(spatialBoundingBox);
        if (sidSet!= null && !sidSet.isEmpty()) {
            for (String sid : sidSet) {
                if (seriesStore.get(sid).getHeadChunk().getChunk().get(0).getTimestamp() <= stopTime) {
                    resultChunks.add(seriesStore.get(sid).getHeadChunk());
                }
            }
        }

        // 2. check disk/immutable part
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForSpatialTemporal(new SpatialTemporalRangeQueryPredicate(startTime, stopTime, spatialBoundingBox.getLowerLeft(), spatialBoundingBox.getUpperRight()));
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            String[] items = blockId.split("\\.");
            Chunk chunk = immutableMemoryRegion.getImmutableMemory().get(items[0]).get(items[1]);
            resultChunks.add(chunk);
        }

        return resultChunks;
    }

    public boolean flush() {
        // 1. flush chunks in immutable region
        immutableMemoryRegion.flush();

        // 2. flush active head chunk
        Set<String> seriesKeys = seriesStore.keySet();
        for (String seriesKey : seriesKeys) {
            SingleSeries series = seriesStore.get(seriesKey);
            Chunk headChunk = series.getHeadChunk();
            String uri = persistenceDriver.getRootUri() + File.separator + headChunk.getSid() + "." + headChunk.getChunkId();
            persistenceDriver.flush(uri, headChunk.toString());
        }

        // TODO update index for head chunk

        return true;
    }

    public void loadAndRebuild() {
        // TODO
    }

    public void stop() {
        // 1. flush all memory data
        flush();

    }

    public SpatialTemporalTree getIndexForImmutableChunks() {
        return indexForImmutableChunks;
    }

    public ImmutableMemoryRegion getImmutableMemoryRegion() {
        return immutableMemoryRegion;
    }

    @Override
    public String toString() {
        return "SeriesMemoryStore{" +
                "seriesStore=" + seriesStore +
                ", immutableMemoryRegion=" + immutableMemoryRegion +
                '}';
    }
}
