package com.rogerguo.test.index;

/**
 * @Description
 * @Date 2021/3/16 14:26
 * @Created by X1 Carbon
 */
public class TemporalIndexNodeTuple extends NodeTuple {

    private String deviceID;

    private long startTimestamp;

    private long stopTimestamp;

    public TemporalIndexNodeTuple(long startTimestamp, long stopTimestamp, String deviceID, String blockPointer) {
        super(blockPointer);
        this.deviceID = deviceID;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
    }

    public TemporalIndexNodeTuple() {}

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

    public String getDeviceID() {
        return deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    @Override
    public String toString() {
        return "(" +
                "deviceID=" + deviceID +
                ", start=" + startTimestamp +
                ", stop=" + stopTimestamp +
                ", block=" + super.getBlockId() +
                ")" ;
    }
}
