package com.rogerguo.test.compression;

import com.rogerguo.test.util.CurveUtil;
import com.rogerguo.test.util.ZCurve;

/**
 * @Description
 * @Date 2021/6/16 22:22
 * @Created by X1 Carbon
 */
public class Test {

    public static void main(String[] args) {
    /*           latitude        longitude
                -10.93934291	-37.0600867
                -10.93964144	-37.0595031
                -10.93985791	-37.05912132
                -10.94007729	-37.058729
     */

    ZCurve zCurve = new ZCurve();
    int lat1 = NormalizedDimensionHelper.normalizedLat(-10.93934291);
    int lon1 = NormalizedDimensionHelper.normalizedLon(-37.0600867);
    long code1 = zCurve.getCurveValue(lon1, lat1);
    System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(code1)));

    int lat2 = NormalizedDimensionHelper.normalizedLat(-10.93964144);
    int lon2 = NormalizedDimensionHelper.normalizedLon(-37.0595031);
    long code2 = zCurve.getCurveValue(lon2, lat2);
    System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(code2)));


    int lat3 = NormalizedDimensionHelper.normalizedLat(-10.93985791);
    int lon3 = NormalizedDimensionHelper.normalizedLon(-37.05912132);
    long code3 = zCurve.getCurveValue(lon3, lat3);
    System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(code3)));
    }

}
