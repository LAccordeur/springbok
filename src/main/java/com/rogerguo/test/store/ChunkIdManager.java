package com.rogerguo.test.store;

import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.compression.NormalizedDimensionHelper;
import com.rogerguo.test.storage.flush.S3LayoutSchema;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.util.ZCurve;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description direct use current timestamp as chunk id
 * @Date 2021/4/25 18:05
 * @Created by GUO Yang
 */
public class ChunkIdManager {

    private Map<String, Integer> chunkIdManager = new HashMap<>();

    private ZCurve zCurve = new ZCurve();

    private DimensionNormalizer dimensionNormalizer = new DimensionNormalizer(-180d, 180d, -90d, 90d);

    private S3LayoutSchema s3LayoutSchema = null;

    private String timestampMode = "from-data"; // two mode: from data or from system

    public ChunkIdManager() { }

    public ChunkIdManager(DimensionNormalizer dimensionNormalizer) {
        this.dimensionNormalizer = dimensionNormalizer;
    }

    public ChunkIdManager(S3LayoutSchema s3LayoutSchema) {
        this.s3LayoutSchema = s3LayoutSchema;
    }

    public ChunkIdManager(DimensionNormalizer dimensionNormalizer, S3LayoutSchema s3LayoutSchema) {
        this.dimensionNormalizer = dimensionNormalizer;
        this.s3LayoutSchema = s3LayoutSchema;
    }

    public ChunkIdManager(DimensionNormalizer dimensionNormalizer, S3LayoutSchema s3LayoutSchema, String timestampMode) {
        this.dimensionNormalizer = dimensionNormalizer;
        this.s3LayoutSchema = s3LayoutSchema;
        this.timestampMode = timestampMode;
    }

    public String getChunkIdWithTags(String sid, List<TrajectoryPoint> pointList) {

        int count;
        if (chunkIdManager.containsKey(sid)) {
            int value = chunkIdManager.get(sid);
            if (value >= Integer.MAX_VALUE) {
                chunkIdManager.put(sid, 0);
                count = 0;
            } else {
                chunkIdManager.put(sid, value + 1);
                count = value + 1;
            }
        } else {
            chunkIdManager.put(sid, 0);
            count = 0;
        }
        String chunkId = "";


        TrajectoryPoint point = pointList.get(pointList.size() / 2);
        long spatialEncoding = zCurve.getCurveValue(dimensionNormalizer.normalizeDimensionX(point.getLongitude()), dimensionNormalizer.normalizeDimensionY(point.getLatitude()));
        long timestampFromData = point.getTimestamp(); // unit should be ms
        // chunk id format: {timestamp}.{spatialEncoding}.{count}
        if (s3LayoutSchema == null) {
            if ("from-data".equals(timestampMode)) {
                chunkId = timestampFromData + "." + spatialEncoding + "." + count;
            } else {
                chunkId = System.currentTimeMillis() + "." + spatialEncoding + "." + count;
            }
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.DIRECT)) {
            chunkId = "" + count;
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.TEMPORAL )  || s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
            /*if ("from-data".equals(timestampMode)) {
                chunkId = timestampFromData + "." + count;
            } else {
                chunkId = System.currentTimeMillis() + "." + count;
            }*/
            if ("from-data".equals(timestampMode)) {
                chunkId = timestampFromData + "." + spatialEncoding + "." + count;
            } else {
                chunkId = System.currentTimeMillis() + "." + spatialEncoding + "." + count;
            }
        } else {
            if ("from-data".equals(timestampMode)) {
                chunkId = timestampFromData + "." + spatialEncoding + "." + count;
            } else {
                chunkId = System.currentTimeMillis() + "." + spatialEncoding + "." + count;
            }
        }
        return chunkId;
    }


    public static String generateStringBlockId(Chunk chunk) {
        return chunk.getSid() + "." + chunk.getChunkId();
    }

}
