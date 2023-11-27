package com.rogerguo.test.index;

/**
 * @Description
 * @Date 2021/3/16 14:32
 * @Created by Yang GUO
 */
public class NodeTuple {

    private String blockId;

    private NodeType nodeType;

    public NodeTuple(String blockId) {
        this.blockId = blockId;
    }

    public NodeTuple() {}

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    @Override
    public String toString() {
        return "NodeTuple{" +
                "blockId='" + blockId + '\'' +
                ", nodeType=" + nodeType +
                '}';
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

}
