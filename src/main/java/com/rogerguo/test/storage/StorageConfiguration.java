package com.rogerguo.test.storage;

import com.rogerguo.test.storage.flush.S3LayoutSchema;
import software.amazon.awssdk.regions.Region;

/**
 * @author yangguo
 * @create 2021-10-05 10:27 AM
 **/
public class StorageConfiguration {

    private Region regionForS3;

    private String bucketNameForS3;

    private int flushBlockNumThresholdForMem;

    private int flushTimeThresholdForMem;

    private int flushTimeThresholdForDisk;

    private int flushBlockNumThresholdForDisk;

    private S3LayoutSchema s3LayoutSchema;

    private String pathNameForDiskTier;

    public StorageConfiguration(Region regionForS3, String bucketNameForS3, int flushBlockNumThresholdForMem, int flushTimeThresholdForMem, int flushTimeThresholdForDisk, int flushBlockNumThresholdForDisk, S3LayoutSchema s3LayoutSchema, String pathNameForDiskTier) {
        this.regionForS3 = regionForS3;
        this.bucketNameForS3 = bucketNameForS3;
        this.flushBlockNumThresholdForMem = flushBlockNumThresholdForMem;
        this.flushTimeThresholdForMem = flushTimeThresholdForMem;
        this.flushTimeThresholdForDisk = flushTimeThresholdForDisk;
        this.flushBlockNumThresholdForDisk = flushBlockNumThresholdForDisk;
        this.s3LayoutSchema = s3LayoutSchema;
        this.pathNameForDiskTier = pathNameForDiskTier;
    }

    @Deprecated
    public StorageConfiguration(Region regionForS3, String bucketNameForS3, int flushBlockNumThresholdForMem, int flushTimeThresholdForMem, int flushTimeThresholdForDisk, int flushBlockNumThresholdForDisk) {
        this.regionForS3 = regionForS3;
        this.bucketNameForS3 = bucketNameForS3;
        this.flushBlockNumThresholdForMem = flushBlockNumThresholdForMem;
        this.flushTimeThresholdForMem = flushTimeThresholdForMem;
        this.flushTimeThresholdForDisk = flushTimeThresholdForDisk;
        this.flushBlockNumThresholdForDisk = flushBlockNumThresholdForDisk;
    }

    public S3LayoutSchema getS3LayoutSchema() {
        return s3LayoutSchema;
    }

    public void setS3LayoutSchema(S3LayoutSchema s3LayoutSchema) {
        this.s3LayoutSchema = s3LayoutSchema;
    }

    public String getPathNameForDiskTier() {
        return pathNameForDiskTier;
    }

    public void setPathNameForDiskTier(String pathNameForDiskTier) {
        this.pathNameForDiskTier = pathNameForDiskTier;
    }

    public int getFlushTimeThresholdForMem() {
        return flushTimeThresholdForMem;
    }

    public void setFlushTimeThresholdForMem(int flushTimeThresholdForMem) {
        this.flushTimeThresholdForMem = flushTimeThresholdForMem;
    }

    public int getFlushTimeThresholdForDisk() {
        return flushTimeThresholdForDisk;
    }

    public void setFlushTimeThresholdForDisk(int flushTimeThresholdForDisk) {
        this.flushTimeThresholdForDisk = flushTimeThresholdForDisk;
    }

    public Region getRegionForS3() {
        return regionForS3;
    }

    public void setRegionForS3(Region regionForS3) {
        this.regionForS3 = regionForS3;
    }

    public String getBucketNameForS3() {
        return bucketNameForS3;
    }

    public void setBucketNameForS3(String bucketNameForS3) {
        this.bucketNameForS3 = bucketNameForS3;
    }

    public int getFlushBlockNumThresholdForMem() {
        return flushBlockNumThresholdForMem;
    }

    public void setFlushBlockNumThresholdForMem(int flushBlockNumThresholdForMem) {
        this.flushBlockNumThresholdForMem = flushBlockNumThresholdForMem;
    }

    public int getFlushBlockNumThresholdForDisk() {
        return flushBlockNumThresholdForDisk;
    }

    public void setFlushBlockNumThresholdForDisk(int flushBlockNumThresholdForDisk) {
        this.flushBlockNumThresholdForDisk = flushBlockNumThresholdForDisk;
    }
}
