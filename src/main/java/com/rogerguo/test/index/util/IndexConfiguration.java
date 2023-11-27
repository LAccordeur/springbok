package com.rogerguo.test.index.util;

import software.amazon.awssdk.regions.Region;

/**
 * @author yangguo
 * @create 2021-09-14 4:52 PM
 **/
public class IndexConfiguration {

    private int blockSize;  // now use the number as block size

    private boolean lazyParentUpdateForActiveNode = true;

    private String bucketNameInS3;

    private String rootDirnameInBucket;

    private Region regionS3;

    private boolean isUsePreciseSpatialIndex = true;

    private boolean isEnableSpatialIndex = true;

    private boolean isEnableRecorderForRecovery = false;

    public IndexConfiguration() {}

    public IndexConfiguration(int blockSize, boolean lazyParentUpdateForActiveNode, String bucketNameInS3, String rootDirnameInBucket, Region regionS3, boolean isUsePreciseSpatialIndex) {
        this.blockSize = blockSize;
        this.lazyParentUpdateForActiveNode = lazyParentUpdateForActiveNode;
        this.bucketNameInS3 = bucketNameInS3;
        this.rootDirnameInBucket = rootDirnameInBucket;
        this.regionS3 = regionS3;
        this.isUsePreciseSpatialIndex = isUsePreciseSpatialIndex;
    }

    public IndexConfiguration(int blockSize, boolean lazyParentUpdateForActiveNode, String bucketNameInS3, String rootDirnameInBucket, Region regionS3, boolean isUsePreciseSpatialIndex, boolean isEnableSpatialIndex) {
        this.blockSize = blockSize;
        this.lazyParentUpdateForActiveNode = lazyParentUpdateForActiveNode;
        this.bucketNameInS3 = bucketNameInS3;
        this.rootDirnameInBucket = rootDirnameInBucket;
        this.regionS3 = regionS3;
        this.isUsePreciseSpatialIndex = isUsePreciseSpatialIndex;
        this.isEnableSpatialIndex = isEnableSpatialIndex;
    }

    public IndexConfiguration(int blockSize, boolean lazyParentUpdateForActiveNode, String bucketNameInS3, String rootDirnameInBucket, Region regionS3, boolean isUsePreciseSpatialIndex, boolean isEnableSpatialIndex, boolean isEnableRecorderForRecovery) {
        this.blockSize = blockSize;
        this.lazyParentUpdateForActiveNode = lazyParentUpdateForActiveNode;
        this.bucketNameInS3 = bucketNameInS3;
        this.rootDirnameInBucket = rootDirnameInBucket;
        this.regionS3 = regionS3;
        this.isUsePreciseSpatialIndex = isUsePreciseSpatialIndex;
        this.isEnableSpatialIndex = isEnableSpatialIndex;
        this.isEnableRecorderForRecovery = isEnableRecorderForRecovery;
    }

    public Region getRegionS3() {
        return regionS3;
    }

    public void setRegionS3(Region regionS3) {
        this.regionS3 = regionS3;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public boolean isLazyParentUpdateForActiveNode() {
        return lazyParentUpdateForActiveNode;
    }

    public void setLazyParentUpdateForActiveNode(boolean lazyParentUpdateForActiveNode) {
        this.lazyParentUpdateForActiveNode = lazyParentUpdateForActiveNode;
    }

    public String getBucketNameInS3() {
        return bucketNameInS3;
    }

    public void setBucketNameInS3(String bucketNameInS3) {
        this.bucketNameInS3 = bucketNameInS3;
    }

    public String getRootDirnameInBucket() {
        return rootDirnameInBucket;
    }

    public void setRootDirnameInBucket(String rootDirnameInBucket) {
        this.rootDirnameInBucket = rootDirnameInBucket;
    }

    public boolean isUsePreciseSpatialIndex() {
        return isUsePreciseSpatialIndex;
    }

    public void setUsePreciseSpatialIndex(boolean usePreciseSpatialIndex) {
        isUsePreciseSpatialIndex = usePreciseSpatialIndex;
    }

    public boolean isEnableSpatialIndex() {
        return isEnableSpatialIndex;
    }

    public void setEnableSpatialIndex(boolean enableSpatialIndex) {
        isEnableSpatialIndex = enableSpatialIndex;
    }

    public boolean isEnableRecorderForRecovery() {
        return isEnableRecorderForRecovery;
    }

    public void setEnableRecorderForRecovery(boolean enableRecorderForRecovery) {
        isEnableRecorderForRecovery = enableRecorderForRecovery;
    }
}
