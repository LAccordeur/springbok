package com.rogerguo.test.store;

public class StoreConfig {

    public static String S3_OBJECT_ACCESS_MODE = "batch";   // batch mode use async requests; direct mode use sync request (SPATIO_TEMPORAL LAYOUT has a bug in the batch mode)

    public static int S3_OBJECT_FLUSH_BATCH_REQUEST_NUM = 100;

    public static boolean IS_USE_GLOBAL_MAPPING_TABLE = false;  // at the beginning of development stage, the tiered storage manager maintains a global mapping for ease-to-use, now we abandon it since the manintaince overhead

    public static boolean ENABLE_META_OBJECT_CACHE_INIT = true;     // init the object metadata cache when starting the store

    public static String META_OBJECT_CACHE_INIT_STORAGE_OPTION = "mem"; // disk or mem (disk is slow)


    public static boolean ENABLE_OBJECT_STORE_DATA_OBJECT_CACHE = false; // only for the get() in ObjectStoreStorageLayer

    public static int OBJECT_STORE_DATA_OBJECT_CACHE_SIZE = 100;    // only for the get() in ObjectStoreStorageLayer

    public static boolean ENABLE_OBJECT_STORE_PARTIAL_GET = false;   // only for the get() in ObjectStoreStorageLayer



    public static boolean ENABLE_BATCH_GET_OPERATION = true;

    public static boolean ENABLE_STORE_CACHE = false;       // this is the cache of series store


    public static boolean ENABLE_HEAD_CHUNK_INDEX = true;

    public static boolean ENABLE_IMMUTABLE_CHUNK_INDEX = true;

    public static boolean ENABLE_INDEX_NODE_CACHE = false;

    public static int NODE_CACHE_SIZE = 2000;

    public static boolean IS_ENABLE_DISK_STORAGE_FOR_INDEX_NODE = false;

    public static int SPATIAL_BITMAP_X_BIT_NUM = 32;

    public static int SPATIAL_BITMAP_Y_BIT_NUM = 32;


    public static boolean ENABLE_DISK_ORDERED_FLUSH = true; // only can be enabled when using spatio-temporal data layout

    public static int DISK_BLOCK_MERGE_THRESHOLD = 1;

    public static int IMMUTABLE_CHUNK_CAPACITY = 100;

    public static int OBJECT_SIZE = 2000;   // the chunk num in a object


}
