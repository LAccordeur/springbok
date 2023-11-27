package com.rogerguo.test.compression;

import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.util.ParseTrajectoryFromDataset;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-06-18 7:46 PM
 **/
public class TestGorillaCompressor {

/*    public static void main(String[] args) {
        System.out.println("two time list:");
        testTrajectoryTimeLongList();
        System.out.println("time list + lat long list: ");
        testTrajectoryLatLongList();
        System.out.println("time list + lat double list: ");
        testTrajectoryLatDoubleList();
        System.out.println("time list + lat merged long list: ");
        testTrajectoryMergedLatLongList();
        System.out.println("time list + lon long list: ");
        testTrajectoryLonLongList();
        System.out.println("time list + lon double list: ");
        testTrajectoryLonDoubleList();
        System.out.println("time list + lon merged long list: ");
        testTrajectoryMergedLonLongList();
        System.out.println("time list + merged lat lon list: ");
        testTrajectoryMergedLongList();
        //test2();
    }

    public static void testTrajectoryTimeLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Integer>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedValues(trajectoryPointList);

        List<Integer> lonValueList = valueLists.get("lon");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(lonValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < lonValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putLong(now + i*60);
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < lonValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getLong());
        }

        c.close();
        bbLat.flip();
        System.out.println(lonValueList.size());
        System.out.println(output.getLongArray().length);
    }

    public static void testTrajectoryMergedLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        List<Long> valueList =  LonLatTransformation.transfer2EncodedValues(trajectoryPointList);

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(valueList.size() * 2*Long.BYTES);

        for(int i = 0; i < valueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putLong(valueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < valueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getLong());
        }


        c.close();
        bbLat.flip();
        System.out.println(valueList.size());
        System.out.println(output.getLongArray().length);

    }

    public static void testTrajectoryMergedLonLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Long>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedAndMergedValues(trajectoryPointList);

        List<Long> lonValueList = valueLists.get("lon");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(lonValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < lonValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putLong(lonValueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < lonValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getLong());
        }

        c.close();
        bbLat.flip();
        System.out.println(lonValueList.size());
        System.out.println(output.getLongArray().length);

    }

    public static void testTrajectoryMergedLatLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Long>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedAndMergedValues(trajectoryPointList);

        List<Long> lonValueList = valueLists.get("lat");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(lonValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < lonValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putLong(lonValueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < lonValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getLong());
        }

        c.close();
        bbLat.flip();
        System.out.println(lonValueList.size());
        System.out.println(output.getLongArray().length);

    }


    public static void testTrajectoryLonLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Integer>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedValues(trajectoryPointList);

        List<Integer> lonValueList = valueLists.get("lon");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(lonValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < lonValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putLong(lonValueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < lonValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getLong());
        }

        c.close();
        bbLat.flip();
        System.out.println(lonValueList.size());
        System.out.println(output.getLongArray().length);

    }

    public static void testTrajectoryLatLongList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Integer>> valueLists =  LonLatTransformation.transfer2SeperatedEncodedValues(trajectoryPointList);

        List<Integer> latValueList = valueLists.get("lat");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(latValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < latValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putLong(latValueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < latValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getLong());
        }

        c.close();
        bbLat.flip();
        System.out.println(latValueList.size());
        System.out.println(output.getLongArray().length);

    }

    public static void testTrajectoryLatDoubleList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Double>> valueLists =  ParseTrajectoryFromDataset.getValueList(trajectoryPointList);

        List<Double> latValueList = valueLists.get("lat");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(latValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < latValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putDouble(latValueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < latValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getDouble());
        }

        c.close();
        bbLat.flip();
        System.out.println(latValueList.size());
        System.out.println(output.getLongArray().length);

    }

    public static void testTrajectoryLonDoubleList() {
        String filePath = "src/main/resources/dataset/20070804033032.plt";
        List<TrajectoryPoint> trajectoryPointList = ParseTrajectoryFromDataset.parseTrajectory(filePath, 6);
        Map<String, List<Double>> valueLists =  ParseTrajectoryFromDataset.getValueList(trajectoryPointList);

        List<Double> lonValueList = valueLists.get("lon");

        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bbLat = ByteBuffer.allocateDirect(lonValueList.size() * 2*Long.BYTES);

        for(int i = 0; i < lonValueList.size(); i++) {
            bbLat.putLong(now + i*60);
            bbLat.putDouble(lonValueList.get(i));
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);
        bbLat.flip();

        for(int j = 0; j < lonValueList.size(); j++) {
            c.addValue(bbLat.getLong(), bbLat.getDouble());
        }

        c.close();
        bbLat.flip();
        System.out.println(lonValueList.size());
        System.out.println(output.getLongArray().length);

    }

    public static void test1() {
        long now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(now, output);

        c.addValue(System.currentTimeMillis(), 22);
        c.close();
        long[] result = output.getLongArray();
        System.out.println();
    }

    public static void test2() {
        int amountOfPoints = 1116;
        long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        LongArrayOutput output = new LongArrayOutput();

        long now = blockStart + 60;
        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putLong(now + i*60);
            bb.putDouble(i * Math.random());
        }

        GorillaCompressor c = new GorillaCompressor(blockStart, output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        System.out.println(output.getLongArray().length);
    }

    static void testEncodeSimilarFloats() {
        long now = LocalDateTime.of(2015, Month.MARCH, 02, 00, 00).toInstant(ZoneOffset.UTC).toEpochMilli();

        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(now, output);

        ByteBuffer bb = ByteBuffer.allocate(5 * 2*Long.BYTES);

        bb.putLong(now + 1);
        bb.putDouble(6.00065e+06);
        bb.putLong(now + 2);
        bb.putDouble(6.000656e+06);
        bb.putLong(now + 3);
        bb.putDouble(6.000657e+06);
        bb.putLong(now + 4);
        bb.putDouble(6.000659e+06);
        bb.putLong(now + 5);
        bb.putDouble(6.000661e+06);

        bb.flip();

        for(int j = 0; j < 5; j++) {
            c.addValue(bb.getLong(), bb.getDouble());
        }

        c.close();

        bb.flip();

        System.out.println(output.getLongArray().length);
    }*/

}
