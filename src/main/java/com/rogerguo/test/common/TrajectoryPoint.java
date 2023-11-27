package com.rogerguo.test.common;

/**
 * @Description
 * @Date 2021/4/25 10:03
 * @Created by GUO Yang
 */
public class TrajectoryPoint extends Point {

    private String oid;

    private long timestamp;

    private String payload;

    public TrajectoryPoint() {}

    public TrajectoryPoint(String oid, long timestamp, double longitude, double latitude) {
        super(longitude, latitude);
        this.oid = oid;
        this.timestamp = timestamp;
    }

    public TrajectoryPoint(String oid, long timestamp, double longitude, double latitude, String payload) {
        super(longitude, latitude);
        this.oid = oid;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

    public String getOid() {
        return oid;
    }

    public long getTimestamp() {
        return timestamp;
    }


    @Override
    public String toString() {
        return "TrajectoryPoint{" +
                "oid='" + oid + '\'' +
                ", longitude='" + super.getLongitude() + '\'' +
                ", latitude='" + super.getLatitude() + '\'' +
                ", timestamp=" + timestamp + '\'' +
                ", payload=" + payload +
                "} ";
    }
}
