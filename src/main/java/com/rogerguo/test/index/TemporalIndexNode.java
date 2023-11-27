package com.rogerguo.test.index;

import com.rogerguo.test.index.predicate.BasicQueryPredicate;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/3/15 14:51
 * @Created by Yang GUO
 */
public class TemporalIndexNode extends TreeNode {

    private List<TemporalIndexNodeTuple> tuples;

    public TemporalIndexNode(TreeNode parentNode, SpatialTemporalTree indexTree) {
        super(parentNode, indexTree);
        this.tuples = new ArrayList<TemporalIndexNodeTuple>();
    }

    public TemporalIndexNode() {}

    /**
     * search tuples according to temporal information and device id
     * @param temporalPredicate
     * @return
     */
    public List<NodeTuple> search(BasicQueryPredicate temporalPredicate) {

        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();

        IdTemporalQueryPredicate idTemporalQueryPredicate = null;
        if (temporalPredicate instanceof IdTemporalQueryPredicate) {
            idTemporalQueryPredicate = (IdTemporalQueryPredicate) temporalPredicate;
        } else {
            return resultTuples;
        }

        for (TemporalIndexNodeTuple indexNodeTuple : this.tuples) {
            if (isIntersected(indexNodeTuple, idTemporalQueryPredicate)) {
                resultTuples.add(indexNodeTuple);
            }
        }

        return resultTuples;
    }

    public boolean insert(NodeTuple tuple) {
        return true;
    }

    public boolean insert(TrajectorySegmentMeta meta) {

        TemporalIndexNodeTuple tuple = new TemporalIndexNodeTuple(meta.getStartTimestamp(), meta.getStopTimestamp(), meta.getDeviceId(), meta.getBlockId());
        tuple.setNodeType(NodeType.DATA);
        tuples.add(tuple);
        this.getIndexTree().temporalEntryNum++;
        return true;
    }

    public long calculateStartTimestamp() {
        long startTimestamp = tuples.get(0).getStartTimestamp();
        for (TemporalIndexNodeTuple tuple : tuples) {
            if (tuple.getStartTimestamp() < startTimestamp) {
                startTimestamp = tuple.getStartTimestamp();
            }
        }
        return startTimestamp;
    }

    public long calculateStopTimestamp() {
        return tuples.get(tuples.size()-1).getStopTimestamp();
    }

    @Override
    public void print() {
        System.out.println(toString());
    }

    @Override
    public String toString() {
        return tuples + "\n ";
    }

    public List<TemporalIndexNodeTuple> getTuples() {
        return tuples;
    }

    private boolean isIntersected(TemporalIndexNodeTuple temporalTuple, IdTemporalQueryPredicate meta) {

        boolean result = false;

        long startTimePredicate = meta.getStartTimestamp();
        long stopTimePredicate = meta.getStopTimestamp();

        if (temporalTuple.getStartTimestamp() <= stopTimePredicate && temporalTuple.getStopTimestamp() >= startTimePredicate && temporalTuple.getDeviceID().equals(meta.getDeviceId()) ){
            result = true;
        }

        return result;
    }

    public void setTuples(List<TemporalIndexNodeTuple> tuples) {
        this.tuples = tuples;
    }
}
