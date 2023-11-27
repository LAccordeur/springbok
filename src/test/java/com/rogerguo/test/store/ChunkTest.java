package com.rogerguo.test.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ChunkTest {

    @Test
    public void deserialize() {


        PortoTaxiRealData data = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");
        TrajectoryPoint point;
        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        List<String> chunkStringList = new ArrayList<>();
        while ((point = data.nextPointFromPortoTaxis()) != null) {
            dataBatch.add(point);
            if (dataBatch.size() == 100) {
                Chunk chunk = new Chunk("testsid", "testchunkid");
                chunk.setChunk(dataBatch);
                String chunkString = Chunk.serialize(chunk);
                chunkStringList.add(chunkString);
                dataBatch.clear();
            }
            if (chunkStringList.size() > 1000) {
                break;
            }
        }

        long start = System.currentTimeMillis();

        for (String chunkString : chunkStringList) {
            Chunk.deserialize(chunkString);
        }
        long stop = System.currentTimeMillis();
        System.out.println("deserialize takes " + (stop - start) + " ms");

    }

    @Test
    public void deserialize2() {
        PortoTaxiRealData data = new PortoTaxiRealData("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");
        TrajectoryPoint point;

        List<String> stringList = new ArrayList<>();
        while ((point = data.nextPointFromPortoTaxis()) != null) {
            stringList.add(point.toString());
            if (stringList.size() > 100000) {
                break;
            }
        }

        long start = System.currentTimeMillis();

        for (String string : stringList) {
            string.split(",");
        }
        long stop = System.currentTimeMillis();
        System.out.println("deserialize takes " + (stop - start) + " ms");
    }

    @Test
    public void deserialize3() {
        PortoTaxiRealData data = new PortoTaxiRealData("/home/ubuntu/data/porto_data_v1_1000w.csv");
        TrajectoryPoint point;

        List<String> stringList = new ArrayList<>();
        while ((point = data.nextPointFromPortoTaxis()) != null) {
            String string = String.format("%s,%f,%f,%d,%s", point.getOid(), point.getLongitude(), point.getLatitude(), point.getTimestamp(), point.getPayload());
            System.out.printf("%d,%d,%d,%d,%d\n", point.getOid().length(), String.valueOf(point.getLongitude()).length(), String.valueOf(point.getLatitude()).length(), String.valueOf(point.getTimestamp()).length(), point.getPayload().length());
            stringList.add(string);
            if (stringList.size() > 100000) {
                break;
            }

        }

        long start = System.currentTimeMillis();

        for (String string : stringList) {
            //string.split(",");
            int length = string.length();
            for (int i = 0; i < string.length(); i+= (length / 5)) {
                string.substring(i, Math.min(string.length(), i + length / 5));
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("deserialize takes " + (stop - start) + " ms");
    }

}