package com.rogerguo.test.common;

/**
 * @author yangguo
 * @create 2021-07-20 8:38 PM
 **/
public class SpatialRange {

    private double min;

    private double max;

    public SpatialRange(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "SpatialRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
