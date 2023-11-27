package com.rogerguo.test.util;

/**
 * @Description
 * @Date 6/4/20 8:51 PM
 * @Created by rogerguo
 */
public class CurveUtil {
    /**
     * big-endian
     * @param val
     * @return
     */
    public static byte[] toBytes(int val) {
        byte[] b = new byte[4];

        for(int i = 3; i > 0; --i) {
            b[i] = (byte)val;
            val >>>= 8;
        }

        b[0] = (byte)val;
        return b;
    }

    public static byte[] toBytes(long val) {

        byte[] b = new byte[8];

        for(int i = 7; i > 0; --i) {
            b[i] = (byte)val;
            val >>>= 8;
        }

        b[0] = (byte)val;
        return b;
    }

    /**
     *
     * @param b big-endian
     * @return
     */
    public static long bytesToLong(byte[] b) {
        if (b.length == 8) {
            long result =  b[7] & 0xFF |
                    ((long)(b[6] & 0xFF)) << 8 |
                    ((long)(b[5] & 0xFF)) << 16 |
                    ((long)(b[4] & 0xFF)) << 24 |
                    ((long)(b[3] & 0xFF)) << 32 |
                    ((long)(b[2] & 0xFF)) << 40 |
                    ((long)(b[1] & 0xFF)) << 48 |
                    ((long)(b[0] & 0xFF)) << 56;
            return result;
        }


        return -1;
    }

    /**
     *
     * @param b big-endian
     * @return
     */
    public static int bytesToInt(byte[] b) {
        if (b.length == 4) {
            int result =  b[3] & 0xFF |
                    (b[2] & 0xFF) << 8 |
                    (b[1] & 0xFF) << 16 |
                    (b[0] & 0xFF) << 24;
            return result;
        }


        return -1;
    }

    /**
     * byte[] to String
     * @param b
     * @return
     */
    public static String bytesToBit(byte[] b) {
        StringBuilder stringBuilder = new StringBuilder();
        for (byte item : b) {
            stringBuilder = stringBuilder.append(byteToBit(item));
        }

        return stringBuilder.toString();
    }

    /**
     * byte to string
     * @param b
     * @return
     */
    public static String byteToBit(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
    }
}

