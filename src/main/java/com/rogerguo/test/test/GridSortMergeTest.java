package com.rogerguo.test.test;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.compression.NormalizedDimensionHelper;
import com.rogerguo.test.util.ZCurve;

import java.util.ArrayList;
import java.util.List;

/**
 * use sort-merge to reduce the number of grids that represent a trajectory
 * @author yangguo
 * @create 2021-11-23 8:05 PM
 **/
public class GridSortMergeTest {

    private static int spatialShiftLength = 10;

    private static ZCurve zCurve = new ZCurve();

    public static void main(String[] args) {
        System.out.println(generateSuccinctGrids(generateSyntheticData()));
    }

    public static List<Long> generateSyntheticData() {
        List<Long> result = new ArrayList<>();
        for (long i = 0; i < 21; i++) {
            result.add(i);
        }

        return result;
    }

    public static List<GridRepresentation> calculateSpatialRepresentation(List<? extends Point> points) {
        List<Long> basicCodingList = new ArrayList<>();
        for (Point point : points) {
            long coding = zCurve.getCurveValue(NormalizedDimensionHelper.normalizedLon(point.getLongitude())
                    , NormalizedDimensionHelper.normalizedLat(point.getLatitude()));
            basicCodingList.add(coding);
        }

        // check if there is a continous sublist and it starting value is 4m
       return generateSuccinctGrids(basicCodingList);
    }

    public static List<GridRepresentation> generateSuccinctGrids(List<Long> basicCodingList) {
        List<GridRepresentation> resultList = new ArrayList<>();

        for (int i = 0; i < basicCodingList.size(); i++) {
            long value = basicCodingList.get(i);
            if (value % 16 == 0) {
                if (i + 15 < basicCodingList.size() && value + 15 == basicCodingList.get(i + 15)) {
                    GridRepresentation newCoding = new GridRepresentation(value, (byte)2);
                    resultList.add(newCoding);
                    i = i + 15;
                    continue;
                }
            }
            if (value % 4 == 0) {
                if (i + 3 < basicCodingList.size() && value + 3 == basicCodingList.get(i + 3)) {
                    GridRepresentation newCoding = new GridRepresentation(value, (byte)1);
                    resultList.add(newCoding);
                    i = i + 3;
                    continue;
                }
            }
            resultList.add(new GridRepresentation(value, (byte)0));
        }
        return resultList;
    }

    static class GridRepresentation {
        private long gridCoding;

        private byte depth;

        public GridRepresentation(long gridCoding, byte depth) {
            this.gridCoding = gridCoding;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return "GridRepresentation{" +
                    "gridCoding=" + gridCoding +
                    ", depth=" + depth +
                    '}';
        }

        public long getGridCoding() {
            return gridCoding;
        }

        public void setGridCoding(long gridCoding) {
            this.gridCoding = gridCoding;
        }

        public byte getDepth() {
            return depth;
        }

        public void setDepth(byte depth) {
            this.depth = depth;
        }
    }
}
