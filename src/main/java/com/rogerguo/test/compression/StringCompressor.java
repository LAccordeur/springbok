package com.rogerguo.test.compression;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author yangguo
 * @create 2022-10-13 1:41 PM
 **/
public class StringCompressor {


    public static void main(String[] args) throws IOException {
        String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
                + "Snappy, a fast compresser/decompresser.";
        byte[] compressed = snappyCompressString(input);
        System.out.println(snappyUncompressString(compressed));
        System.out.println(Snappy.isValidCompressedBuffer(compressed));
        System.out.println(Snappy.isValidCompressedBuffer(input.getBytes(StandardCharsets.UTF_8)));
    }

    private static void test() throws IOException {
        String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
                + "Snappy, a fast compresser/decompresser.";
        byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
        String compressedString = new String(compressed);
        System.out.println(compressedString);

        byte[] uncompressed = Snappy.uncompress(compressed);

        String result = new String(uncompressed, "UTF-8");
        System.out.println(result);

    }

    public static byte[] snappyCompressString(String data) {
        try {
            return Snappy.compress(data.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            return data.getBytes(StandardCharsets.UTF_8);
        }

    }

    public static String snappyUncompressString(byte[] data) {
        try {
            return new String(Snappy.uncompress(data), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static boolean isValidSnappyCompressedBuffer(byte[] input) {
        try {
            return Snappy.isValidCompressedBuffer(input);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

}
