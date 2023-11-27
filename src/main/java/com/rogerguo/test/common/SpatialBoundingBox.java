package com.rogerguo.test.common;

import com.rogerguo.test.common.Point;

/**
 * @author yangguo
 * @create 2021-07-14 3:59 PM
 **/
public class SpatialBoundingBox {

    private Point lowerLeft;

    private Point upperRight;

    public SpatialBoundingBox() {}

    public SpatialBoundingBox(Point lowerLeft, Point upperRight) {
        this.lowerLeft = lowerLeft;
        this.upperRight = upperRight;
    }

    public Point getLowerLeft() {
        return lowerLeft;
    }

    public void setLowerLeft(Point lowerLeft) {
        this.lowerLeft = lowerLeft;
    }

    public Point getUpperRight() {
        return upperRight;
    }

    public void setUpperRight(Point upperRight) {
        this.upperRight = upperRight;
    }

    /**
     * 范围的开区间闭区间 [right, left)
     * * @param boundingBox
     * @param point
     * @return
     */
    public static boolean checkBoundingBoxContainPoint(SpatialBoundingBox boundingBox, Point point) {

        if (point.getLongitude() >= boundingBox.getLowerLeft().getLongitude() && point.getLongitude() < boundingBox.getUpperRight().getLongitude()
                && point.getLatitude() >= boundingBox.getLowerLeft().getLatitude() && point.getLatitude() < boundingBox.getUpperRight().getLatitude()) {
            return true;
        }

        return false;
    }

    public static SpatialBoundingBox getOverlappedBoundingBox(SpatialBoundingBox boundingBox1, SpatialBoundingBox boundingBox2) {
        Point lowerLeftPoint1 = boundingBox1.getLowerLeft();
        Point upperRightPoint1 = boundingBox1.getUpperRight();

        Point lowerLeftPoint2 = boundingBox2.getLowerLeft();
        Point upperRightPoint2 = boundingBox2.getUpperRight();

        SpatialRange overlappedRangeX = getOverlappedRange(new SpatialRange(lowerLeftPoint1.getLongitude(), upperRightPoint1.getLongitude()), new SpatialRange(lowerLeftPoint2.getLongitude(), upperRightPoint2.getLongitude()));
        SpatialRange overlappedRangeY = getOverlappedRange(new SpatialRange(lowerLeftPoint1.getLatitude(), upperRightPoint1.getLatitude()), new SpatialRange(lowerLeftPoint2.getLatitude(), upperRightPoint2.getLatitude()));

        if (overlappedRangeX != null && overlappedRangeY != null) {
            Point overlappedLowerLeftPoint = new Point(overlappedRangeX.getMin(), overlappedRangeY.getMin());
            Point overlappedUpperRightPoint = new Point(overlappedRangeX.getMax(), overlappedRangeY.getMax());
            return new SpatialBoundingBox(overlappedLowerLeftPoint, overlappedUpperRightPoint);
        }

        return null;
    }

    public static SpatialRange getOverlappedRange(SpatialRange range1, SpatialRange range2) {

        if (range1.getMax() >= range2.getMin() && range1.getMin() <=range2.getMax()) {
            Double overlappedMin = Math.max(range1.getMin(), range2.getMin());
            Double overlappedMax = Math.min(range1.getMax(), range2.getMax());
            return new SpatialRange(overlappedMin, overlappedMax);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "SpatialBoundingBox{" +
                "lowerLeft=" + lowerLeft +
                ", upperRight=" + upperRight +
                '}';
    }
}
