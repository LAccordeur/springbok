package com.rogerguo.test.util;


import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Date 6/4/20 8:52 PM
 * @Created by rogerguo
 */
public class ZCurve implements SpaceFillingCurve {


    @Override
    public String getCurveValueString(int x, int y) {
        long numResult = splitZ2(y) | splitZ2(x) << 1;

        byte[] result = CurveUtil.toBytes(numResult);

        return CurveUtil.bytesToBit(result);
    }

    @Override
    public String getCurveValueString(int x, int y, int z) {
        //long numResult = split(x) | split(y) << 1 | split(z) << 2;
        long numResult = splitZ3(z) | splitZ3(y) << 1 | splitZ3(x) << 2;
        byte[] result = CurveUtil.toBytes(numResult);

        return CurveUtil.bytesToBit(result);
    }

    /**
     *
     * @param x >0 x31x30...x0
     * @param y >0 y31y30...y0
     * @return
     */
    @Override
    public long getCurveValue(int x, int y) {
        return splitZ2(y) | splitZ2(x) << 1;
    }

    @Override
    public Map<String, Integer> from2DCurveValue(long curveValue) {
        Map<String, Integer> resultMap = new HashMap<>();

        int y = combineZ2(curveValue);
        int x = combineZ2(curveValue >> 1);

        resultMap.put("x", x);
        resultMap.put("y", y);

        return resultMap;
    }

    public static void main(String[] args) {
        ZCurve zCurve = new ZCurve();
        long result = zCurve.getCurveValue(3,7);
        Map<String, Integer> m = zCurve.from2DCurveValue(result);
        System.out.println(m);
    }


    @Override
    public long getCurveValue(int x, int y, int z) {
        //long numResult = split(x) | split(y) << 1 | split(z) << 2;
        long numResult = splitZ3(z) | splitZ3(y) << 1 | splitZ3(x) << 2;

        return numResult;
    }


    public Map<String, Integer> from3DCurveValue(long curveValue) {
        Map<String, Integer> resultMap = new HashMap<>();
        int z = combineZ3(curveValue);
        int y = combineZ3(curveValue >> 1);
        int x = combineZ3(curveValue >> 2);
        resultMap.put("x", x);
        resultMap.put("y", y);
        resultMap.put("z", z);

        return resultMap;
    }

    /** come from geomesa **/
    /** insert 00 between every bit in value. Only first 21 bits can be considered. */
    private static long MaxMask = 0x1fffffL;
    private static long splitZ3(long value) {
        long x = value & MaxMask;
        x = (x | x << 32) & 0x1f00000000ffffL;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 16) & 0x1f0000ff0000ffL;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 8)  & 0x100f00f00f00f00fL;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        x = (x | x << 4)  & 0x10c30c30c30c30c3L;
        //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
        return (x | x << 2)      & 0x1249249249249249L;
    }

    private static int combineZ3(long curveValue) {
        long x = curveValue & 0x1249249249249249L;
        x = (x ^ (x >>  2)) & 0x10c30c30c30c30c3L;
        x = (x ^ (x >>  4)) & 0x100f00f00f00f00fL;
        x = (x ^ (x >>  8)) & 0x1f0000ff0000ffL;
        x = (x ^ (x >> 16)) & 0x1f00000000ffffL;
        x = (x ^ (x >> 32)) & MaxMask;
        return (int) x;
    }


    /** insert 0 between every bit in value. Only first 31 bits can be considered. */
    public static long splitZ2(long value) {

        long x = value & 0x7fffffffL;
        x = (x ^ (x << 32)) & 0x00000000ffffffffL;
        x = (x ^ (x << 16)) & 0x0000ffff0000ffffL;
        x = (x ^ (x <<  8)) & 0x00ff00ff00ff00ffL; // 11111111000000001111111100000000..
        x = (x ^ (x <<  4)) & 0x0f0f0f0f0f0f0f0fL; // 1111000011110000
        x = (x ^ (x <<  2)) & 0x3333333333333333L; // 11001100..
        x = (x ^ (x <<  1)) & 0x5555555555555555L; // 1010...
        return x;
    }

    /** combine every other bit to form a value. Maximum value is 31 bits. */
    public static int combineZ2(long z) {
        long x = z & 0x5555555555555555L;
        x = (x ^ (x >>  1)) & 0x3333333333333333L;
        x = (x ^ (x >>  2)) & 0x0f0f0f0f0f0f0f0fL;
        x = (x ^ (x >>  4)) & 0x00ff00ff00ff00ffL;
        x = (x ^ (x >>  8)) & 0x0000ffff0000ffffL;
        x = (x ^ (x >> 16)) & 0x00000000ffffffffL;
        return (int) x;
    }


}

