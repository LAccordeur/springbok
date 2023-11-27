package com.rogerguo.test.index;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.TrajectoryPoint;

import java.util.List;

/**
 * @Description
 * @Date 2021/3/16 15:22
 * @Created by Yang GUO
 */
public class TrajectorySegmentMeta {

    private long startTimestamp;

    private long stopTimestamp;

    private Point lowerLeft;

    private Point upperRight;

    private String deviceId;

    private String blockId;

    private List<TrajectoryPoint> trajectoryPointList;

    public TrajectorySegmentMeta(long startTimestamp, long stopTimestamp, Point lowerLeft, Point upperRight, String deviceId, String blockId, List<TrajectoryPoint> trajectoryPointList) {
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.lowerLeft = lowerLeft;
        this.upperRight = upperRight;
        this.deviceId = deviceId;
        this.blockId = blockId;
        this.trajectoryPointList = trajectoryPointList;
    }

    public TrajectorySegmentMeta(long startTimestamp, long stopTimestamp, Point lowerLeft, Point upperRight, String deviceId, String blockId) {
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.lowerLeft = lowerLeft;
        this.upperRight = upperRight;
        this.deviceId = deviceId;
        this.blockId = blockId;
    }

    public List<TrajectoryPoint> getTrajectoryPointList() {
        return trajectoryPointList;
    }

    public void setTrajectoryPointList(List<TrajectoryPoint> trajectoryPointList) {
        this.trajectoryPointList = trajectoryPointList;
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

    public Point getLowerLeft() {
        return lowerLeft;
    }

    public void setLowerLeft(Point lowerLeft) {
        this.lowerLeft = lowerLeft;
    }

    public Point getUpperRight() {
        return upperRight;
    }

    public void setUpperRight(Point upperRight) {
        this.upperRight = upperRight;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    @Override
    public String toString() {
        return "TrajectorySegmentMeta{" +
                "startTimestamp=" + startTimestamp +
                ", stopTimestamp=" + stopTimestamp +
                ", lowerLeft=" + lowerLeft +
                ", upperRight=" + upperRight +
                ", deviceId='" + deviceId + '\'' +
                ", blockId='" + blockId + '\'' +
                ", trajectoryPointList=" + trajectoryPointList +
                '}';
    }
}
