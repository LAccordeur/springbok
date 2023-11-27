package com.rogerguo.test.compression;

import me.lemire.integercompression.differential.IntegratedIntCompressor;

/**
 * @author yangguo
 * @create 2021-06-17 7:37 PM
 **/
public class TestJavaFastPFOR {

    public static void main(String[] args) {
        IntegratedIntCompressor intCompressor = new IntegratedIntCompressor();
        int[] data = new int[]{2, 2, 3};
        int[] compressed = intCompressor.compress(data);

        int[] recov = intCompressor.uncompress(compressed);
    }

}
