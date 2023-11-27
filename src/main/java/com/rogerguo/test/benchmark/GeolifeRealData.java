package com.rogerguo.test.benchmark;

import com.rogerguo.test.common.PortoTaxiPoint;
import com.rogerguo.test.common.TrajectoryPoint;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeolifeRealData implements DataProducer {


    public String defaultFilename = "";

    public BufferedReader objReader = null;

    public GeolifeRealData(String defaultFilename) {
        this.defaultFilename = defaultFilename;

        try {
            objReader = new BufferedReader(new FileReader(defaultFilename), 1024 * 1024 * 4);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public TrajectoryPoint nextPointFromGeolife() {

        int count = 0;
        TrajectoryPoint point = null;
        try {
            String strCurrentLine;

            while ((strCurrentLine = objReader.readLine()) != null) {
                count++;


                String[] items = strCurrentLine.split(",");
                if (items.length != 7) {
                    continue;
                }

                String userId = items[0];
                double longitude = Double.parseDouble(items[1]);
                double latitude = Double.parseDouble(items[2]);
                //String dummy = items[3];
                //double altitude = Double.parseDouble(items[4]);
                //String dateDays = items[5];
                //long timestamp = Long.parseLong(items[6]) * 1000; // unit is ms
                long timestamp = Long.parseLong(items[6]);  // unit is ms (we changed the dataset format so its orignal timeunit is ms)

                TrajectoryPoint portoTaxiPoint = new TrajectoryPoint(userId, timestamp, longitude, latitude, strCurrentLine);

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
        return nextPointFromGeolife();
    }
}
