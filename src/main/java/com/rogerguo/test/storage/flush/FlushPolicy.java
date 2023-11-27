package com.rogerguo.test.storage.flush;

import com.rogerguo.test.storage.BlockLocation;
import com.rogerguo.test.storage.StorageLayerName;
import com.rogerguo.test.storage.driver.PersistenceDriver;
import com.rogerguo.test.storage.layer.StorageLayer;

/**
 * @author yangguo
 * @create 2021-09-20 11:00 AM
 **/
public abstract class FlushPolicy {

    private StorageLayerName flushToWhichStorageLayerName;

    public FlushPolicy(StorageLayerName flushToWhichStorageLayerName) {
        this.flushToWhichStorageLayerName = flushToWhichStorageLayerName;
    }

    public abstract void flush(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer);

    public StorageLayerName getFlushToWhichStorageLayerName() {
        return flushToWhichStorageLayerName;
    }

}
