package com.rogerguo.test.index;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @Description
 * @Date 2021/3/16 14:27
 * @Created by X1 Carbon
 */
public class InternalNodeTuple extends NodeTuple {

    private long startTimestamp;

    private long stopTimestamp;

    @JsonIgnore
    private TreeNode nodePointer;

    public InternalNodeTuple() {
    }

    public InternalNodeTuple(String blockPointer, long startTimestamp, long stopTimestamp, TreeNode nodePointer) {
        super(blockPointer);
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.nodePointer = nodePointer;
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

    public TreeNode getNodePointer() {
        return nodePointer;
    }

    public void setNodePointer(TreeNode nodePointer) {
        this.nodePointer = nodePointer;
    }

    @Override
    public String toString() {
        return "(" +
                "start=" + startTimestamp +
                ", stop=" + stopTimestamp +
                ", block=" + nodePointer.getBlockId() +
                ")";
    }
}
