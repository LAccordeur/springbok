package com.rogerguo.test.index;

import com.rogerguo.test.common.SpatialBoundingBox;

/**
 * @Description
 * @Date 2021/3/16 14:26
 * @Created by Yang GUO
 */
public class SpatialIndexNodeTuple extends NodeTuple {

    private long spatialGridId;

    private String deviceId;

    private SpatialBoundingBox boundingBox;

    private long startTimestamp;

    private long stopTimestamp;

    private byte[] signature;

    public SpatialIndexNodeTuple(String blockPointer, long spatialGridId, String deviceId) {
        super(blockPointer);
        this.spatialGridId = spatialGridId;
        this.deviceId = deviceId;
    }

    public SpatialIndexNodeTuple(String blockId, String deviceId, SpatialBoundingBox boundingBox, long startTimestamp, long stopTimestamp, byte[] signature) {
        super(blockId);
        this.deviceId = deviceId;
        this.boundingBox = boundingBox;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.signature = signature;
    }

    public SpatialIndexNodeTuple(String blockId, String deviceId, SpatialBoundingBox boundingBox, long startTimestamp, long stopTimestamp) {
        super(blockId);
        this.deviceId = deviceId;
        this.boundingBox = boundingBox;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
    }

    public SpatialIndexNodeTuple() {}

    public long getSpatialGridId() {
        return spatialGridId;
    }

    public void setSpatialGridId(long spatialGridId) {
        this.spatialGridId = spatialGridId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public SpatialBoundingBox getBoundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(SpatialBoundingBox boundingBox) {
        this.boundingBox = boundingBox;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }

    public byte[] getSignature() {
        return signature;
    }

    public void setSignature(byte[] signature) {
        this.signature = signature;
    }

    @Override
    public String toString() {
        return "(" +
                "deviceId=" + deviceId +
                ", gridId=" + spatialGridId +
                ", block=" + super.getBlockId() +
                ")" ;
    }
}
