package com.rogerguo.test.storage;

/**
 * @author yangguo
 * @create 2021-10-26 4:40 PM
 **/
public class BlockIdentifierEntity{

    private String sid;

    private long timestamp;

    private long spatialPointEncoding;

    private int count;

    private long spatialPartitionId;

    private long temporalPartitionId;

    private int spatialPointLon;

    private int spatialPointLat;

    public static BlockIdentifierEntity decoupleBlockIdForSpatioTemporalLayout(String blockId) {
        String[] items = blockId.split("\\.");
        return new BlockIdentifierEntity(items[0], Long.parseLong(items[1]), Long.parseLong(items[2]), Integer.parseInt(items[3]));
    }


    public static String coupleBlockIdForSpatioTemporalLayout(BlockIdentifierEntity entity) {
        return entity.getSid() + "." + entity.getTimestamp() + "." + entity.getSpatialPointEncoding() + "." + entity.getCount();
    }

    public static BlockIdentifierEntity decoupleBlockIdForSingleTrajectoryLayout(String blockId) {
        /*String[] items = blockId.split("\\.");
        return new BlockIdentifierEntity(items[0], Long.valueOf(items[1]), 0, Integer.parseInt(items[2]));*/
        String[] items = blockId.split("\\.");
        return new BlockIdentifierEntity(items[0], Long.parseLong(items[1]), Long.parseLong(items[2]), Integer.parseInt(items[3]));
    }

    public static String coupleBlockIdForSingleTrajectoryLayout(BlockIdentifierEntity entity) {
        //return entity.getSid() + "." + entity.getTimestamp() + "." + entity.getCount();
        return entity.getSid() + "." + entity.getTimestamp() + "." + entity.getSpatialPointEncoding() + "." + entity.getCount();
    }

    public static BlockIdentifierEntity decoupleBlockIdForTemporalLayout(String blockId) {
        String[] items = blockId.split("\\.");
        return new BlockIdentifierEntity(items[0], Long.parseLong(items[1]), 0, Integer.parseInt(items[2]));
    }

    public static String coupleBlockIdForTemporalLayout(BlockIdentifierEntity entity) {
        return entity.getSid() + "." + entity.getTimestamp() + "." + entity.getCount();
    }

    public BlockIdentifierEntity(String sid, long timestamp, long spatialPointEncoding, int count) {
        this.sid = sid;
        this.timestamp = timestamp;
        this.spatialPointEncoding = spatialPointEncoding;
        this.count = count;
    }

    public int getSpatialPointLon() {
        return spatialPointLon;
    }

    public int getSpatialPointLat() {
        return spatialPointLat;
    }

    public void setSpatialPointLon(int spatialPointLon) {
        this.spatialPointLon = spatialPointLon;
    }

    public void setSpatialPointLat(int spatialPointLat) {
        this.spatialPointLat = spatialPointLat;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSpatialPointEncoding() {
        return spatialPointEncoding;
    }

    public void setSpatialPointEncoding(long spatialPointEncoding) {
        this.spatialPointEncoding = spatialPointEncoding;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getSpatialPartitionId() {
        return spatialPartitionId;
    }

    public void setSpatialPartitionId(long spatialPartitionId) {
        this.spatialPartitionId = spatialPartitionId;
    }

    @Override
    public String toString() {
        return "BlockIdentifierEntity{" +
                "sid='" + sid + '\'' +
                ", timestamp=" + timestamp +
                ", spatialPointEncoding=" + spatialPointEncoding +
                ", count=" + count +
                ", spatialPartitionId=" + spatialPartitionId +
                ", temporalPartitionId=" + temporalPartitionId +
                ", spatialPointLon=" + spatialPointLon +
                ", spatialPointLat=" + spatialPointLat +
                '}';
    }
}
