package com.rogerguo.test.storage.flush;

/**
 * for ObjectStorageLayer, ToS3FlushPolicy
 * @author yangguo
 * @create 2021-11-01 8:12 PM
 **/
public class S3LayoutSchema {

    private S3LayoutSchemaName s3LayoutSchemaName = S3LayoutSchemaName.SPATIO_TEMPORAL;

    private int spatialRightShiftBitNum = 16;

    private int timePartitionLength = 1000 * 60 * 60 * 24;

    public S3LayoutSchema(S3LayoutSchemaName s3LayoutSchemaName, int timePartitionLength) {
        this.s3LayoutSchemaName = s3LayoutSchemaName;
        this.timePartitionLength = timePartitionLength;
    }

    public S3LayoutSchema(S3LayoutSchemaName schemaName, int spatialRightShiftBitNum, int timePartitionLength) {
        this.s3LayoutSchemaName = schemaName;
        this.spatialRightShiftBitNum = spatialRightShiftBitNum;
        this.timePartitionLength = timePartitionLength;
    }

    public S3LayoutSchema(S3LayoutSchemaName s3LayoutSchemaName) {
        this.s3LayoutSchemaName = s3LayoutSchemaName;
    }

    public S3LayoutSchemaName getS3LayoutSchemaName() {
        return s3LayoutSchemaName;
    }

    public void setS3LayoutSchemaName(S3LayoutSchemaName s3LayoutSchemaName) {
        this.s3LayoutSchemaName = s3LayoutSchemaName;
    }

    public int getSpatialRightShiftBitNum() {
        return spatialRightShiftBitNum;
    }

    public void setSpatialRightShiftBitNum(int spatialRightShiftBitNum) {
        this.spatialRightShiftBitNum = spatialRightShiftBitNum;
    }

    public int getTimePartitionLength() {
        return timePartitionLength;
    }

    public void setTimePartitionLength(int timePartitionLength) {
        this.timePartitionLength = timePartitionLength;
    }

    @Override
    public String toString() {
        return "{" +
                "s3LayoutSchemaName=" + s3LayoutSchemaName +
                ", spatialRightShiftBitNum=" + spatialRightShiftBitNum +
                ", timePartitionLength=" + timePartitionLength +
                '}';
    }
}
