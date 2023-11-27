package com.rogerguo.test.benchmark;

import com.rogerguo.test.common.TrajectoryPoint;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-01-10 3:05 PM
 **/
public class BrinkhoffGeneratorData {

    private String filename;

    public BufferedReader objReader = null;

    public BrinkhoffGeneratorData(String filename) {
        this.filename = filename;

        try {
            objReader = new BufferedReader(new FileReader(filename), 1024*1024*4);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BrinkhoffGeneratorData brinkhoffGeneratorData = new BrinkhoffGeneratorData("/home/yangguo/Codes/traj-generator-new/dataset/oldenburg-slow-begin5000.data");
        for (int i = 0; i < 100; i++) {
            System.out.println(brinkhoffGeneratorData.nextPointFromBrinkhoffGenerator());
        }
    }

    public TrajectoryPoint nextPointFromBrinkhoffGenerator() {
        TrajectoryPoint point = null;

        try {
            String strCurrentLine;

            while ((strCurrentLine = objReader.readLine()) != null) {
                String[] items = strCurrentLine.split("\t");
                if (items.length != 10) {
                    continue;
                }

                String pointType = items[0];
                String oid = items[1];
                String seqNumber = items[2];
                String objectClass = items[3];
                long timestamp = Long.parseLong(items[4]);
                double longitude = Double.parseDouble(items[5]);
                double latitude = Double.parseDouble(items[6]);


                TrajectoryPoint dataPoint = new TrajectoryPoint(oid, timestamp, longitude, latitude, strCurrentLine);

                return dataPoint;
            }

            return null;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return point;

    }

    public void close() {
        try {
            if (objReader != null) {
                objReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
