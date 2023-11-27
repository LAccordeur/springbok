package com.rogerguo.test.common;

/**
 * @author yangguo
 * @create 2021-12-30 8:35 PM
 **/
public class DimensionNormalizer {

    public double xOriginalLowBound;

    public double xOriginalHighBound;

    public double yOriginalLowBound;

    public double yOriginalHighBound;

    private CustomNormalizedDimension xNormalizer;

    private CustomNormalizedDimension yNormalizer;

    public DimensionNormalizer(double xOriginalLowBound, double xOriginalHighBound, double yOriginalLowBound, double yOriginalHighBound) {
        this.xOriginalLowBound = xOriginalLowBound;
        this.xOriginalHighBound = xOriginalHighBound;
        this.yOriginalLowBound = yOriginalLowBound;
        this.yOriginalHighBound = yOriginalHighBound;
        this.xNormalizer = new CustomNormalizedDimension(xOriginalLowBound, xOriginalHighBound, CustomNormalizedDimension.precision);
        this.yNormalizer = new CustomNormalizedDimension(yOriginalLowBound, yOriginalHighBound, CustomNormalizedDimension.precision);
    }

    public int normalizeDimensionX(double x) {
        return xNormalizer.normalize(x);
    }

    public int normalizeDimensionY(double y) {
        return yNormalizer.normalize(y);
    }

    public double getxOriginalLowBound() {
        return xOriginalLowBound;
    }

    public void setxOriginalLowBound(double xOriginalLowBound) {
        this.xOriginalLowBound = xOriginalLowBound;
    }

    public double getxOriginalHighBound() {
        return xOriginalHighBound;
    }

    public void setxOriginalHighBound(double xOriginalHighBound) {
        this.xOriginalHighBound = xOriginalHighBound;
    }

    public double getyOriginalLowBound() {
        return yOriginalLowBound;
    }

    public void setyOriginalLowBound(double yOriginalLowBound) {
        this.yOriginalLowBound = yOriginalLowBound;
    }

    public double getyOriginalHighBound() {
        return yOriginalHighBound;
    }

    public void setyOriginalHighBound(double yOriginalHighBound) {
        this.yOriginalHighBound = yOriginalHighBound;
    }

    public CustomNormalizedDimension getxNormalizer() {
        return xNormalizer;
    }

    public void setxNormalizer(CustomNormalizedDimension xNormalizer) {
        this.xNormalizer = xNormalizer;
    }

    public CustomNormalizedDimension getyNormalizer() {
        return yNormalizer;
    }

    public void setyNormalizer(CustomNormalizedDimension yNormalizer) {
        this.yNormalizer = yNormalizer;
    }
}
