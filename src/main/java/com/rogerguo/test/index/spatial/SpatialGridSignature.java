package com.rogerguo.test.index.spatial;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author yangguo
 * @create 2021-12-30 2:27 PM
 **/
public class SpatialGridSignature {


    public static byte[] generateSignature(SpatialBoundingBox boundingBox, List<? extends Point> pointList, int xBitNum, int yBitNum) {
        double lonGridLength = (boundingBox.getUpperRight().getLongitude() - boundingBox.getLowerLeft().getLongitude()) / xBitNum;
        double latGridLength = (boundingBox.getUpperRight().getLatitude() - boundingBox.getLowerLeft().getLatitude()) / yBitNum;
        double baseLongitude = boundingBox.getLowerLeft().getLongitude();
        double baseLatitude = boundingBox.getLowerLeft().getLatitude();

        int bitNum = xBitNum * yBitNum;
        BitSet bitSet = new BitSet(bitNum);
        for (Point point : pointList) {
            int xGridIndex = (int) Math.floor((point.getLongitude() - baseLongitude) / lonGridLength);
            if (xGridIndex >= xBitNum - 1) {
                xGridIndex = xBitNum - 1;
            }
            int yGridIndex = (int) Math.floor((point.getLatitude() - baseLatitude) / latGridLength);
            if (yGridIndex >= yBitNum - 1) {
                yGridIndex = yBitNum - 1;
            }
            int bitmapIndex = xGridIndex * xBitNum + yGridIndex;
            if (bitmapIndex >= bitNum - 1) {    // avoid overflow
                bitmapIndex = bitNum - 1;
            }
            /*System.out.println("x: " + xGridIndex);
            System.out.println("y: " + yGridIndex);
            System.out.println("index: " + bitmapIndex);*/
            bitSet.set(bitmapIndex);
        }

        return bitSet.toByteArray();
    }

    public static BitSet handleVirtualLine(BitSet bitSet, int xBitNum, int yBitNum) {

        List<Integer> markedIndexList = new ArrayList<>();
        int bitNum = xBitNum * yBitNum;
        for (int i = 0; i < bitNum; i++) {
            if (bitSet.get(i)) {
                markedIndexList.add(i);
            }
        }
        if (markedIndexList.size() == 1) {
            return bitSet;
        }

        for (int i = 0; i < markedIndexList.size() - 1; i++) {
            int begin = markedIndexList.get(i);
            int beginX = begin % xBitNum;
            int beginY = begin / xBitNum;

            int end = markedIndexList.get(i+1);
            int endX = end % xBitNum;
            int endY = end / xBitNum;

            for (int xIndex = beginX; xIndex <= endX; xIndex++) {
                for (int yIndex = beginY; yIndex <= endY; yIndex++) {
                    int bitIndex = yIndex * xBitNum + xIndex;
                    bitSet.set(bitIndex);
                }
            }
        }


        return bitSet;
    }

    public static boolean checkOverlap(SpatialBoundingBox predicateBoundingBox, SpatialBoundingBox signatureBoundingBox, byte[] signature, int xBitNum, int yBitNum) {
        BitSet bitSet = BitSet.valueOf(signature);
        SpatialBoundingBox overlappedBoundingBox = SpatialBoundingBox.getOverlappedBoundingBox(predicateBoundingBox, signatureBoundingBox);

        if (overlappedBoundingBox == null) {
            return false;
        }

        double lonGridLength = (signatureBoundingBox.getUpperRight().getLongitude() - signatureBoundingBox.getLowerLeft().getLongitude()) / xBitNum;
        double latGridLength = (signatureBoundingBox.getUpperRight().getLatitude() - signatureBoundingBox.getLowerLeft().getLatitude()) / yBitNum;
        double baseLongitude = signatureBoundingBox.getLowerLeft().getLongitude();
        double baseLatitude = signatureBoundingBox.getLowerLeft().getLatitude();

        int xGridIndexMin = (int) Math.floor((overlappedBoundingBox.getLowerLeft().getLongitude() - baseLongitude) / lonGridLength);
        int xGridIndexMax = Math.min((int) Math.floor((overlappedBoundingBox.getUpperRight().getLongitude() -baseLongitude) / lonGridLength), xBitNum - 1);
        int yGridIndexMin = (int) Math.floor((overlappedBoundingBox.getLowerLeft().getLatitude() - baseLatitude) / latGridLength);
        int yGridIndexMax = Math.min((int) Math.floor((overlappedBoundingBox.getUpperRight().getLatitude() - baseLatitude) / latGridLength), yBitNum - 1);

        for (int x = xGridIndexMin; x <= xGridIndexMax; x++) {
            for (int y = yGridIndexMin; y <= yGridIndexMax; y++) {
                int bitMapIndex = x * xBitNum + y;
                if (bitSet.get(bitMapIndex)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static void main(String[] args) {

        SpatialBoundingBox boundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(1, 1));
        List<Point> pointList = new ArrayList<>();
        Point point = new Point(0.0, 0.0);
        pointList.add(point);
        Point point1 = new Point(0.3, 0.3);
        pointList.add(point1);
        Point point2 = new Point(0.5, 0.5);
        pointList.add(point2);
        Point point3 = new Point(0.99, 0.99);
        pointList.add(point3);
        byte[] result = generateSignature(boundingBox, pointList, 4, 4);

        BitSet bitSet = BitSet.valueOf(result);
        System.out.println(bitSet);

        SpatialBoundingBox predicate = new SpatialBoundingBox(new Point(0.15, 0.75), new Point(0.5, 0.75));

        boolean checkResult = checkOverlap(predicate, boundingBox, result, 4, 4);
        System.out.println("check result: " + checkResult);
    }

}
