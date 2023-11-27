package com.rogerguo.test.test;

import com.rogerguo.test.storage.driver.DiskDriver;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.*;

/**
 * @author yangguo TODO why async has poor performance
 * @create 2023-09-19 7:30 PM
 **/
public class FileOperationTest {

    public static void main(String[] args) {
        mergedReadTest();
    }

    static void mergedReadTest() {
        String file = "/home/ubuntu/data/porto_data_v1.csv";
        DiskDriver diskDriver = new DiskDriver("/home/ubuntu/data/");
        diskDriver.getDataAsStringPartial(file, 0, 4);

        long time1 = System.nanoTime();
        diskDriver.getDataAsStringPartial(file, 16384, 16384 * 4);
        long time2 = System.nanoTime();

        long time3 = System.nanoTime();
        diskDriver.getDataAsStringPartial(file, 16384 * 10, 16384);
        diskDriver.getDataAsStringPartial(file, 16384 * 12, 16384);
        long time4 = System.nanoTime();

        System.out.println("merged: " + (time2 - time1));
        System.out.println("seperated: " + (time4 - time3));

    }

    static String getAlphaNumericString(int n)
    {

        // choose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    static void writeWithOpen() {
        String dataString = getAlphaNumericString(1024 * 4);
        String filename = "/home/ubuntu/projects/new-version-vldb/trajectory-index/test-write.file";
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            writeData(filename, dataString);
        }
        long stop = System.currentTimeMillis();
        System.out.println("write time: " + (stop - start));
    }
    static void writeWithoutOpen() {
        String dataString = getAlphaNumericString(1024 * 4 );
        String filename = "/home/ubuntu/projects/new-version-vldb/trajectory-index/test-write.file";
        File file = new File(filename);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                writeData(filename, dataString);
            }
            long stop = System.currentTimeMillis();
            fileOutputStream.close();
            System.out.println("write time: " + (stop - start));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    static void writeData(String key, String dataString, FileOutputStream stream) {

        try {



            stream.write(dataString.getBytes());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void writeData(String key, String dataString) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                file.createNewFile();

            }

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(dataString.getBytes());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static ByteBuffer getDataBuffer(String c) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4096*64; i++) {
            sb.append(c);
        }
        String str = sb.toString();
        Charset cs = Charset.forName("UTF-8");
        ByteBuffer bb = ByteBuffer.wrap(str.getBytes(cs));

        return bb;
    }

    static void syncWriteTest() {

        File file = new File("/home/ubuntu/projects/new-version-vldb/trajectory-index/test-sync.file");

        try {
            byte[] data = getDataBuffer(String.valueOf(1).substring(0, 1)).array();
            long start = System.currentTimeMillis();
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            for (int i = 0; i < 500; i++) {
                fileOutputStream.write(getDataBuffer(String.valueOf(1).substring(0, 1)).array());
            }
            long stop = System.currentTimeMillis();
            fileOutputStream.close();
            System.out.println("sync write time: " + (stop - start));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    static void asyncWriteTest() {
        Path path = Paths.get("/home/ubuntu/projects/new-version-vldb/trajectory-index/test.file");

        List<Future<Integer>> futureList = new ArrayList<>();
        try {
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, WRITE, CREATE);
            ByteBuffer data = getDataBuffer(String.valueOf(1).substring(0,1));
            long start = System.currentTimeMillis();
            for (int i = 0; i < 500; i++) {
                ByteBuffer byteBuffer = getDataBuffer(String.valueOf(1).substring(0,1));;
                Future<Integer> result = afc.write(byteBuffer, i * 4096 * 64);
                futureList.add(result);
            }

            for (Future<Integer> future : futureList) {
                while (!future.isDone()) {

                }
            }
            long stop = System.currentTimeMillis();
            System.out.println("async write time: " + (stop - start));

            for (Future<Integer> future : futureList) {
                System.out.format("%s bytes written  to  %s%n", future.get(),
                        path.toAbsolutePath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static void syncReadTest() {
        File file = new File("/home/ubuntu/projects/new-version-vldb/trajectory-index/test-sync.file");

        Random random = new Random(1);
        byte[] buffer = new byte[4096 * 64];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                randomAccessFile.seek(random.nextInt(100) * 4096 * 64);
                randomAccessFile.read(buffer, 0, 4096 * 64);
            }
            long stop = System.currentTimeMillis();
            System.out.println("sync read time: " + (stop - start));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    static void asyncReadTest() {
        Path path = Paths.get("/home/ubuntu/projects/new-version-vldb/trajectory-index/test.file");

        List<Future<Integer>> futureList = new ArrayList<>();
        List<ByteBuffer> resultBuffer = new ArrayList<>();


        try {
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, READ);

            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {

                ByteBuffer dataBuffer = ByteBuffer.allocate(4096 * 64);
                Future<Integer> result = afc.read(dataBuffer, i*4096 * 64);
                futureList.add(result);
                //resultBuffer.add(dataBuffer);
            }


            long stop = System.currentTimeMillis();
            for (Future<Integer> future : futureList) {
                while (!future.isDone()) {

                }
            }

            //long stop = System.currentTimeMillis();
            System.out.println("async read time: " + (stop - start));



            for (ByteBuffer buffer : resultBuffer) {
                byte[] byteData = buffer.array();
                Charset cs = Charset.forName("UTF-8");
                String data = new String(byteData, cs);

                //System.out.println(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static void asyncWrite() {
        Path path = Paths.get("/home/yangguo/IdeaProjects/trajectory-index/flush-test/test-sync.file");
        try {
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, WRITE, CREATE);
            String str = "myteststring";
            ByteBuffer byteBuffer = ByteBuffer.wrap(str.getBytes("UTF-8"));
            Future<Integer> result = afc.write(byteBuffer, 0);


            while (!result.isDone()) {

            }

            System.out.println("the write bytes: " + result.get());

        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    static void asyncRead() {
        Path path = Paths.get("/home/yangguo/IdeaProjects/trajectory-index/flush-test/test.file");

        try {
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, READ);
            ByteBuffer dataBuffer = ByteBuffer.allocate(1024);

            Future<Integer> result = afc.read(dataBuffer, 0);

            int readBytes = result.get();


            System.out.format("%s bytes read   from  %s%n", readBytes, path);
            System.out.format("Read data is:%n");

            byte[] byteData = dataBuffer.array();
            Charset cs = Charset.forName("UTF-8");
            String data = new String(byteData, cs);

            System.out.println(data);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}