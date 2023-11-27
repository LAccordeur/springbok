package com.rogerguo.test.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * which layer of a block store in
 * @author yangguo
 * @create 2021-09-20 11:40 AM
 **/
public class BlockLocation {

    @JsonIgnore
    private StorageLayerName storageLayerName;

    private String filepath;

    private int offset;

    private int length;

    @JsonIgnore
    private boolean inS3 = false;

    @JsonIgnore
    private boolean range = false;

    @JsonIgnore
    private String blockId;

    public BlockLocation(String filepath, boolean range) {
        this.filepath = filepath;
        this.range = range;
    }

    public BlockLocation() {}

    public BlockLocation(int offset, int length) {
        this.offset = offset;
        this.length = length;
    }

    public BlockLocation(StorageLayerName storageLayerName) {
        this.storageLayerName = storageLayerName;
    }

    public BlockLocation(StorageLayerName storageLayerName, String filepath, int offset, int length) {
        this.storageLayerName = storageLayerName;
        this.filepath = filepath;
        this.offset = offset;
        this.length = length;
    }

    public BlockLocation(String filepath, int offset, int length) {
        this.filepath = filepath;
        this.offset = offset;
        this.length = length;
    }

    public BlockLocation(StorageLayerName storageLayerName, int offset, int length) {
        this.storageLayerName = storageLayerName;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public String toString() {
        return "BlockLocation{" +
                "storageLayerName=" + storageLayerName +
                ", filepath='" + filepath + '\'' +
                ", offset=" + offset +
                ", length=" + length +
                '}';
    }

    public boolean isRange() {
        return range;
    }

    public void setRange(boolean range) {
        this.range = range;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public StorageLayerName getStorageLayerName() {
        return storageLayerName;
    }

    public void setStorageLayerName(StorageLayerName storageLayerName) {
        this.storageLayerName = storageLayerName;
    }

    public boolean isInS3() {
        return inS3;
    }

    public void setInS3(boolean inS3) {
        this.inS3 = inS3;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }
}
