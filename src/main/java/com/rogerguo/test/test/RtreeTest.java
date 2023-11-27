package com.rogerguo.test.test;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.rogerguo.test.common.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2021-12-07 9:04 PM
 **/
public class RtreeTest {

    public static List<Point> generatePoints(int listSize) {
        List<Point> pointList = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < listSize; i++) {
            pointList.add(new Point(random.nextInt(1000), random.nextInt(2000)));
        }

        return pointList;
    }

    public static void main(String[] args) {
        long total = 0;
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();
            RTree<String, Geometry> rtree = RTree.star().create();
            for (Point point : generatePoints(10000)) {
                rtree = rtree.add("test", Geometries.point(point.getLongitude(), point.getLatitude()));
            }
            long stop = System.currentTimeMillis();
            System.out.println("time: " + (stop - start));
            total = total + (stop - start);
        }
        System.out.println("average: " + (total / 100.0));

    }

}
