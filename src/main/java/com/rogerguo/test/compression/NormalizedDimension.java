package com.rogerguo.test.compression;

/**
 * @Description refer to GeoMesa
 * @Date 6/4/20 8:45 PM
 * @Created by rogerguo
 */
public class NormalizedDimension {

    public static final int precision = 21;

    private double min; // Min value considered for normalization

    private double max;

    private int maxIndex; // Max value to normalize to

    private long bins;

    private double normalizer;

    private double denormalizer;

    public NormalizedDimension() {}

    public NormalizedDimension(double min, double max, int precision) {
        this.min = min;
        this.max = max;

        this.bins = 1L << precision;
        this.normalizer = bins / (max - min);
        this.denormalizer = (max - min) / bins;
        this.maxIndex = (int)(bins - 1);
    }

    /**
     * Normalize the value
     *
     * @param x [min, max]
     * @return [0, maxIndex]
     */
    public int normalize(double x) {
        if (x >= max) {
            return maxIndex;
        } else {
            return (int) Math.floor((x - min) * normalizer);
        }
    }

    /**
     * Denormalize the value in bin x
     *
     * @param x [0, maxIndex]
     * @return [min, max]
     */
    public double denormalize(int x) {
        if (x >= maxIndex) {
            return min + (maxIndex + 0.5d) * denormalizer;
        } else {
            return min + (x + 0.5d) * denormalizer;
        }
    }

}