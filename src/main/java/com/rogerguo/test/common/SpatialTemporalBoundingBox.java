package com.rogerguo.test.common;

/**
 * @author yangguo
 * @create 2021-09-13 9:16 PM
 **/
public class SpatialTemporalBoundingBox extends SpatialBoundingBox{

    private long startTime;

    private long stopTime;

    public SpatialTemporalBoundingBox(Point lowerLeft, Point upperRight, long startTime, long stopTime) {
        super(lowerLeft, upperRight);
        this.startTime = startTime;
        this.stopTime = stopTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    @Override
    public String toString() {
        return "SpatialTemporalBoundingBox{" +
                "lowerLeft=" + super.getLowerLeft() +
                ", upperRight=" + super.getUpperRight() +
                ", startTime=" + startTime +
                ", stopTime=" + stopTime +
                '}';
    }
}
