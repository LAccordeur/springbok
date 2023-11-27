package com.rogerguo.test.index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.rogerguo.test.index.predicate.BasicQueryPredicate;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;

import java.util.List;

/**
 * @Description
 * @Date 2021/3/15 15:16
 * @Created by Yang GUO
 */
public class TreeNode {

    @JsonIgnore
    private TreeNode parentNode;

    @JsonIgnore
    private SpatialTemporalTree indexTree;

    private String blockId;  // id of block where stores this tree node

    private boolean isFlushed = false;

    public TreeNode(TreeNode parentNode, SpatialTemporalTree indexTree) {
        this.parentNode = parentNode;
        this.indexTree = indexTree;
    }

    public TreeNode() {
    }


    public boolean isFlushed() {
        return isFlushed;
    }

    public void setFlushed(boolean flushed) {
        isFlushed = flushed;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public TreeNode getParentNode() {
        return parentNode;
    }

    public void setParentNode(TreeNode parentNode) {
        this.parentNode = parentNode;
    }

    public SpatialTemporalTree getIndexTree() {
        return indexTree;
    }

    public void setIndexTree(SpatialTemporalTree indexTree) {
        this.indexTree = indexTree;
    }


    /**
     *
     * @param meta
     * @return
     */
    List<NodeTuple> search(BasicQueryPredicate meta) {
        return null;
    }

    List<NodeTuple> searchForIdTemporal(IdTemporalQueryPredicate predicate) {
        return null;
    }

    List<NodeTuple> searchForSpatialTemporal(SpatialTemporalRangeQueryPredicate predicate) {
        return null;
    }

    /**
     * for internal node
     * @param tuple
     * @param treeNodeOfThisTuple  the corresponding child node of the @tuple. This is used to update child node's parent attribute when
     * @return
     */
/*    boolean insert(NodeTuple tuple, TreeNode treeNodeOfThisTuple) {
        return false;
    }*/


    /**
     * for internal node
     * @param tuple
     * @return
     */
    boolean insert(NodeTuple tuple) {
        return false;
    }

    /*
    for leaf node
     */
    boolean insert(TrajectorySegmentMeta meta){
        return false;
    }

    /**
     * for leaf node (with a policy that update parent using a lazy way)
     * @param meta
     * @return
     */
    boolean insertForLazyParentUpdate(TrajectorySegmentMeta meta) {
        return false;
    }

    void print() {
        System.out.println(toString());
    }

    @Override
    public String toString() {
        return "TreeNode{" +
                "blockId=" + blockId +
                '}';
    }
}
