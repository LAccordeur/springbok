package com.rogerguo.test.benchmark;

import com.rogerguo.test.common.PortoTaxiPoint;
import com.rogerguo.test.common.TrajectoryPoint;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2021-12-15 4:26 PM
 **/
public class PortoTaxiRealData implements DataProducer {

    public static void main(String[] args) {
        String defaultFilename = "/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv";
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(defaultFilename);
        TrajectoryPoint point = portoTaxiRealData.nextPointFromPortoTaxis();
        int count = 0;
        System.out.println(point);
        long start = System.currentTimeMillis();
        while (point != null) {
            count++;
            if (count % 100000 == 0) {
                System.out.println(count);
                System.out.println(point);
            }
            point = portoTaxiRealData.nextPointFromPortoTaxis();
        }
        long stop = System.currentTimeMillis();

        System.out.println("takes " + (stop - start) + " ms");

    }

    /**
     * NEIST: A Neural-Enhanced Index for Spatio-Temporal Queries
     * https://www.kaggle.com/crailtap/taxi-trajectory
     * @param filename
     */

    public String defaultFilename = "/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/train_flat_sorted.csv";

    public BufferedReader objReader = null;

    public PortoTaxiRealData(String defaultFilename) {
        this.defaultFilename = defaultFilename;

        try {
            objReader = new BufferedReader(new FileReader(defaultFilename), 1024 * 1024 * 4);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    public static List<TrajectoryPoint> generateFullPointsFromPortoTaxis(String filename) {
        List<TrajectoryPoint> resultPoints = new ArrayList<>();

        int count = 0;
        try {
            BufferedReader dataReader = new BufferedReader(new FileReader(filename), 1024 * 1024 * 4);
            String strCurrentLine;

            while ((strCurrentLine = dataReader.readLine()) != null) {
                count++;
                if (count % 10000 == 0) {
                    System.out.println(count);
                }
                //System.out.println(strCurrentLine);
                String[] items = strCurrentLine.split(",");
                if (items.length != 10) {
                    continue;
                }

                String tripId = items[0];
                String callType = items[1];
                int originCall = -1;
                if (items[2] != null && !items[2].equals("")) {
                    originCall = Integer.parseInt(items[2]);
                }
                int originStand = -1;
                if (items[3] != null && !items[3].equals("")) {
                    originStand = Integer.parseInt(items[3]);
                }
                String taxiId = items[4];
                long timestamp = 0;
                if (items[5] != null && !items[5].equals("")) {
                    timestamp = Long.parseLong(items[5]) * 1000;  // unit is ms
                }
                String dayType = items[6];
                boolean missingData = Boolean.parseBoolean(items[7]);
                if (missingData) {
                    continue;
                }
                double longitude = Double.parseDouble(items[8]);
                double latitude = Double.parseDouble(items[9]);

                PortoTaxiPoint portoTaxiPoint = new PortoTaxiPoint(taxiId, timestamp, longitude, latitude, tripId, callType, originCall, originStand, dayType, missingData);
                resultPoints.add(portoTaxiPoint);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultPoints;
    }

    public TrajectoryPoint nextPointFromPortoTaxis() {

        int count = 0;
        TrajectoryPoint point = null;
        try {
            String strCurrentLine;

            while ((strCurrentLine = objReader.readLine()) != null) {
                count++;


                String[] items = strCurrentLine.split(",");
                if (items.length != 10) {
                    continue;
                }

                String tripId = items[0];
                String callType = items[1];
                int originCall = -1;
                if (items[2] != null && !items[2].equals("")) {
                    originCall = Integer.parseInt(items[2]);
                }
                int originStand = -1;
                if (items[3] != null && !items[3].equals("")) {
                    originStand = Integer.parseInt(items[3]);
                }
                String taxiId = items[4];
                long timestamp = 0;
                if (items[5] != null && !items[5].equals("")) {
                    //timestamp = Long.parseLong(items[5]) * 1000;  // unit is ms
                    timestamp = Long.parseLong(items[5]);  // unit is ms (we changed the dataset format so its orignal timeunit is ms)
                }
                String dayType = items[6];
                boolean missingData = Boolean.parseBoolean(items[7]);
                if (missingData) {
                    continue;
                }
                double longitude = Double.parseDouble(items[8]);
                double latitude = Double.parseDouble(items[9]);

                TrajectoryPoint portoTaxiPoint = new TrajectoryPoint(taxiId, timestamp, longitude, latitude, strCurrentLine);

                return portoTaxiPoint;
            }

            return null;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return point;
    }

    public void close() {
        try {
            if (objReader != null)
                objReader.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public TrajectoryPoint nextPoint() {
        return this.nextPointFromPortoTaxis();
    }
}
