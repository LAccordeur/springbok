package com.rogerguo.test.storage.flush;

/**
 * @author yangguo
 * @create 2021-11-01 3:29 PM
 **/
public enum S3LayoutSchemaName {
    /**
     * DIRECT: each chunk is an individual object
     * SINGLE_TRAJECTORY: each object contains chunks from the same trajectory
     * TEMPORAL: all chunks in the same time partition is an object
     * SPATIO_TEMPORAL: all chunks in the same time and spatial partition is an object
     */
    DIRECT, SINGLE_TRAJECTORY, TEMPORAL, SPATIO_TEMPORAL_STR, SPATIO_TEMPORAL
}

