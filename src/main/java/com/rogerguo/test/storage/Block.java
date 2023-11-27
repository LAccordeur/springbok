package com.rogerguo.test.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;

/**
 * @author yangguo
 * @create 2021-09-29 5:07 PM
 **/
public class Block {

    private String blockId;

    private String dataString;

    private String metaDataString;

    @JsonIgnore
    private byte[] dataBytes;

    public Block() { }

    public Block(String blockId, String dataString) {
        this.blockId = blockId;
        this.dataString = dataString;
    }

    public Block(String blockId, byte[] dataBytes) {
        this.blockId = blockId;
        this.dataBytes = dataBytes;
    }

    public Block(String blockId, String dataString, String metaDataString) {
        this.blockId = blockId;
        this.dataString = dataString;
        this.metaDataString = metaDataString;
    }

    public String getMetaDataString() {
        return metaDataString;
    }

    public void setMetaDataString(String metaDataString) {
        this.metaDataString = metaDataString;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getDataString() {
        return dataString;
    }

    public void setDataString(String dataString) {
        this.dataString = dataString;
    }

    public byte[] getDataBytes() {
        return dataBytes;
    }

    public void setDataBytes(byte[] dataBytes) {
        this.dataBytes = dataBytes;
    }

    @Override
    public String toString() {
        return "Block{" +
                "blockId='" + blockId + '\'' +
                ", dataString='" + dataString + '\'' +
                ", metaDataString='" + metaDataString + '\'' +
                '}';
    }
}
