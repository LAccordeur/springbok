package com.rogerguo.test.benchmark;

/**
 * @author yangguo
 * @create 2021-12-27 2:27 PM
 **/
public class MemoryUsageUtils {

    public static long getMaxMemory() {
        return Runtime.getRuntime().maxMemory();
    }

    public static long getUsedMemory() {
        return getTotalMemory() - getFreeMemory();
    }

    public static long getTotalMemory() {
        return Runtime.getRuntime().totalMemory();
    }

    public static long getFreeMemory() {
        return Runtime.getRuntime().freeMemory();
    }


}
