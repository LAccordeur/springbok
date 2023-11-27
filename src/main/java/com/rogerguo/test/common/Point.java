package com.rogerguo.test.common;

/**
 * @author yangguo
 * @create 2021-06-24 5:17 PM
 **/
public class Point {

    private double longitude;

    private double latitude;

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public Point() {}

    public Point(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public String toString() {
        return "(" +
                longitude + ", " + latitude +
                ")";
    }
}
