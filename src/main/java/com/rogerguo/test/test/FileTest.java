package com.rogerguo.test.test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author yangguo
 * @create 2021-09-28 2:34 PM
 **/
public class FileTest {

    public static void main(String[] args) {
        readData("/home/yangguo/IdeaProjects/trajectory-index/src/main/resources/s3/2MB.file");
    }

    public static void readData(String filename) {
        long start = System.nanoTime();
        File file = new File(filename);
        long length = file.length();

        int lengthInt = (int) length;

        byte[] buffer = new byte[lengthInt];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(0);
            randomAccessFile.read(buffer, 0, lengthInt);

        } catch (IOException e) {
            e.printStackTrace();
        }
        long stop = System.nanoTime();


        System.out.println("time: " + (stop - start));
    }

}
