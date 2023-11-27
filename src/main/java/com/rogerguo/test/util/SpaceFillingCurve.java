package com.rogerguo.test.util;

/**
 * @Description
 * @Date 6/4/20 8:49 PM
 * @Created by rogerguo
 */
import java.util.Map;

public interface SpaceFillingCurve {

    /**
     * long (byte[]) to string
     * @param x
     * @param y
     * @return
     */
    String getCurveValueString(int x, int y);

    /**
     * long (byte[]) to string
     * @param x
     * @param y
     * @param z
     * @return
     */
    String getCurveValueString(int x, int y, int z);

    /**
     * only consider first 21 bits
     * @param x >0 x31x30...x0
     * @param y >0 y31y30...y0
     * @return
     */
    long getCurveValue(int x, int y);

    /**
     * only consider first 21 bits x20y20z20x19y19z19
     * @param x >0 x20x19...x0
     * @param y >0 y20y19...y0
     * @param z >0 z20z19...z0
     * @return
     */
    long getCurveValue(int x, int y, int z);

    Map<String, Integer> from3DCurveValue(long curveValue);

    Map<String, Integer> from2DCurveValue(long curveValue);

}
