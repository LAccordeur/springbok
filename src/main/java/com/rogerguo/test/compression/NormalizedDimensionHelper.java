package com.rogerguo.test.compression;


/**
 * @Description The max precision of normalization (lat, lon, time) is 21 bits
 * @Date 2019/11/7 20:34
 * @Created by GUO Yang
 */
public class NormalizedDimensionHelper {


    private static NormalizedDimension dimensionLat = new NormalizedDimension(-90d, 90d, NormalizedDimension.precision);

    private static NormalizedDimension dimensionLon = new NormalizedDimension(-180d, 180d, NormalizedDimension.precision);


    public static int normalizedLat(double lat) {

        return dimensionLat.normalize(lat);
    }

    public static int normalizedLon(double lon) {
        return dimensionLon.normalize(lon);
    }


    public static void main(String[] args) {
        System.out.println(normalizedLon(-37.0600867));
        System.out.println(normalizedLon(-37.0595031));
        System.out.println(normalizedLon(-37.05912132));
    }
}
