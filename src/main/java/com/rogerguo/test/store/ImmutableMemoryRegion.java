package com.rogerguo.test.store;

import com.rogerguo.test.storage.driver.PersistenceDriver;

import java.io.File;
import java.util.*;

/**
 * @Description
 * @Date 2021/4/25 10:02
 * @Created by GUO Yang
 */
@Deprecated
public class ImmutableMemoryRegion {

    private Map<String, Map<String, Chunk>> immutableMemory;    // use a hash map to store immutable chunks (key is the series id, value is the map of immutalbe chunk (key is chunk id, value is chunk itself))

    private PersistenceDriver persistenceDriver;

    private int maxMemorySize;  // TODO

    public ImmutableMemoryRegion(PersistenceDriver persistenceDriver) {
        this.immutableMemory = new HashMap<>();
        this.persistenceDriver = persistenceDriver;
    }

    public boolean moveToImmutableMemoryRegion(String sid, Chunk chunk) {
        if (immutableMemory.containsKey(sid)) {
            immutableMemory.get(sid).put(chunk.getChunkId(), chunk);
        } else {
            // this is a new series added to immutable region
            Map<String, Chunk> chunkMap = new HashMap<>();
            chunkMap.put(chunk.getChunkId(), chunk);
            immutableMemory.put(sid, chunkMap);
        }

        return true;
    }

    public Map<String, Map<String, Chunk>> getImmutableMemory() {
        return immutableMemory;
    }

    public boolean flush() {

        Set<String> seriesKeys = immutableMemory.keySet();

        for (String seriesKey : seriesKeys) {
            Map<String, Chunk> chunkMap = immutableMemory.get(seriesKey);

            for (Chunk chunk : chunkMap.values()) {
                String uri = persistenceDriver.getRootUri() + File.separator + chunk.getSid() + "." + chunk.getChunkId();
                persistenceDriver.flush(uri, chunk.toString());
            }
        }


        return true;
    }

    @Override
    public String toString() {
        return "ImmutableMemoryRegion{" +
                "immutableMemory=" + immutableMemory +
                '}';
    }
}
