package com.rogerguo.test.index.predicate;

/** basic query predicate which only provide time range predicate
 * @author yangguo
 * @create 2021-06-24 5:08 PM
 **/
public class BasicQueryPredicate {

    private long startTimestamp;

    private long stopTimestamp;

    public BasicQueryPredicate() {}

    public BasicQueryPredicate(long startTimestamp, long stopTimestamp) {
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }

    @Override
    public String toString() {
        return "BasicQueryPredicate{" +
                "startTimestamp=" + startTimestamp +
                ", stopTimestamp=" + stopTimestamp +
                '}';
    }
}
