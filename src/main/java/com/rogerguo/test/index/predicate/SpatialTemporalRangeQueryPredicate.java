package com.rogerguo.test.index.predicate;


import com.rogerguo.test.common.Point;

/**
 * @author yangguo
 * @create 2021-06-24 5:14 PM
 **/
public class SpatialTemporalRangeQueryPredicate extends BasicQueryPredicate{

    private Point lowerLeft;

    private Point upperRight;

    public SpatialTemporalRangeQueryPredicate() {}

    public SpatialTemporalRangeQueryPredicate(long startTimestamp, long stopTimestamp, Point lowerLeft, Point upperRight) {
        super(startTimestamp, stopTimestamp);
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

    @Override
    public String toString() {
        return "SpatialTemporalRangeQueryPredicate{" +
                "lowerLeft=" + lowerLeft +
                ", upperRight=" + upperRight +
                "} " + super.toString();
    }
}
