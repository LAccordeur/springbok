package com.rogerguo.test.benchmark;

import com.rogerguo.test.common.TrajectoryPoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GeolifeRealDataTest {

    @Test
    void nextPointFromGeolife() {
        String filename = "/home/ubuntu/data1/Geolife_v1.csv";
        GeolifeRealData realData = new GeolifeRealData(filename);

        TrajectoryPoint point;
        while ((point = realData.nextPointFromGeolife()) != null) {
            System.out.println(point);
        }

    }
}