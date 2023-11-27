package com.rogerguo.test.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.PortoTaxiPoint;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.TrajectorySegmentMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/4/25 10:28
 * @Created by GUO Yang
 */
public class Chunk {

    private String sid;   // the sid of series that the chunk belongs to

    private String chunkId;

    private TrajectorySegmentMeta meta;

    private List<TrajectoryPoint> chunk;

    @JsonIgnore
    private int dataSize;

    public Chunk() {}

    public Chunk(String sid, String chunkId) {
        this.sid = sid;
        this.chunkId = chunkId;
        this.chunk = new ArrayList<>();
    }

    public Chunk(String sid) {
        this.sid = sid;
        this.chunk = new ArrayList<>();
    }

    private static ObjectMapper objectMapper = new ObjectMapper();
    public static String serialize(Chunk chunk) {
        String result = "";
        /*try {
            result = objectMapper.writeValueAsString(chunk);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }*/

        StringBuilder stringBuilder = new StringBuilder();

        TrajectorySegmentMeta meta = chunk.getMeta();
        if (meta == null) {
            meta = SingleSeries.generateIndexEntry(chunk);
        }
        String metaString = meta.getStartTimestamp() + ";" + meta.getStopTimestamp() + ";" + meta.getLowerLeft().getLongitude() + ";" + meta.getLowerLeft().getLatitude()
                + ";" + meta.getUpperRight().getLongitude() + ";" + meta.getUpperRight().getLatitude() + "\n";
        stringBuilder.append(metaString);

        for (TrajectoryPoint point : chunk.getChunk()) {
            //String string = String.format("%s;%f;%f;%d;%s\n", point.getOid(), point.getLongitude(), point.getLatitude(), point.getTimestamp(), point.getPayload());
            String string = point.getOid() + ";" + point.getLongitude() + ";" + point.getLatitude() + ";" + point.getTimestamp() + ";" + point.getPayload() + "\n";
            stringBuilder.append(string);
        }

        return stringBuilder.toString();
    }

    public static Chunk deserialize(String data) {

        Chunk chunk = new Chunk();
        /*try {

            chunk = objectMapper.readValue(data, Chunk.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }*/
        List<TrajectoryPoint> pointList = new ArrayList<>();
        chunk.setChunk(pointList);

        String[] records = data.split("\n");
        String metaString = records[0];
        String[] metaItems = metaString.split(";");
        TrajectorySegmentMeta meta = new TrajectorySegmentMeta(Long.parseLong(metaItems[0]), Long.parseLong(metaItems[1]), new Point(Double.parseDouble(metaItems[2]), Double.parseDouble(metaItems[3])),
                new Point(Double.parseDouble(metaItems[4]), Double.parseDouble(metaItems[5])), "", "");
        chunk.setMeta(meta);
        for (int i = 1; i < records.length; i++) {
            String[] items = records[i].split(";");
            if (items.length == 5) {
                TrajectoryPoint point = new TrajectoryPoint(items[0], Long.parseLong(items[3]), Double.parseDouble(items[1]), Double.parseDouble(items[2]), items[4]);
                pointList.add(point);
            }
        }

        return chunk;
    }

    public List<TrajectoryPoint> getChunk() {
        return chunk;
    }

    public int size() {
        return chunk.size();
    }

    public boolean add(TrajectoryPoint point) {
        return chunk.add(point);
    }

    public String getSid() {
        return sid;
    }

    public String getChunkId() {
        return chunkId;
    }

    public void setChunk(List<TrajectoryPoint> chunk) {
        this.chunk = chunk;
    }

    public void setChunkId(String chunkId) {
        this.chunkId = chunkId;
    }

    public TrajectorySegmentMeta getMeta() {
        return meta;
    }

    public void setMeta(TrajectorySegmentMeta meta) {
        this.meta = meta;
    }

    @Override
    public String toString() {
        return "{meta: " + "(sid: " + sid + ", chunkId: " + chunkId + ")  data: " +chunk + "}";
    }
}
