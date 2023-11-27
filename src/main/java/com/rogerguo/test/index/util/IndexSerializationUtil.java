package com.rogerguo.test.index.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.index.*;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;


/**
 * @author yangguo
 * @create 2021-08-02 9:20 PM
 **/
public class IndexSerializationUtil {

    public static final String ROOT_SPATIAL_FILENAME = "root.spatial.node";

    public static final String ROOT_TEMPORAL_FILENAME = "root.temporal.node";

    public static final String ROOT_INTERNAL_FILENAME = "root.node";

    public static final String TREE_META = "index-tree.meta";

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        SpatialIndexNode spatialIndexNode = new SpatialIndexNode();

        spatialIndexNode.setTuples(new ArrayList<>());
        spatialIndexNode.getTuples().add(new SpatialIndexNodeTuple( null, 55, "d"));
        spatialIndexNode.getTuples().add(new SpatialIndexNodeTuple( null, 123, "d"));
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(spatialIndexNode);
        System.out.println(json);

        SpatialIndexNode back = mapper.readValue(json, SpatialIndexNode.class);
        System.out.println();
    }

    /**
     * contain blockSize, nodeCount, rootType, rootNodeBlockId
     * @param tree
     * @return
     */
    public static String serializeIndexTreeMeta(SpatialTemporalTree tree) {
        String result = "";
        try {
            result = objectMapper.writeValueAsString(tree);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static SpatialTemporalTree deserializeIndexTreeMeta(String content) {
        SpatialTemporalTree indexTree = null;

        try {
            indexTree = objectMapper.readValue(content, SpatialTemporalTree.class);
            indexTree.setSpatialTwoLevelGridIndex(new TwoLevelGridIndex());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return indexTree;
    }

    public static String serializeInternalNode(InternalNode internalNode) {
        String result = "";
        try {
            result = objectMapper.writeValueAsString(internalNode);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static InternalNode deserializeInternalNode(String content) {
        InternalNode internalNode = null;

        try {
            internalNode = objectMapper.readValue(content, InternalNode.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return internalNode;
    }

    public static String serializeLeafSpatialNode(SpatialIndexNode spatialIndexNode) {
        String result = "";
        try {
            result = objectMapper.writeValueAsString(spatialIndexNode);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static SpatialIndexNode deserializeLeafSpatialNode(String content) {
        SpatialIndexNode spatialIndexNode = null;

        try {
            spatialIndexNode = objectMapper.readValue(content, SpatialIndexNode.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return spatialIndexNode;
    }

    public static String serializeLeafTemporalNode(TemporalIndexNode temporalIndexNode) {
        String result = "";
        try {
            result = objectMapper.writeValueAsString(temporalIndexNode);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static TemporalIndexNode deserializeLeafTemporalNode(String content) {
        TemporalIndexNode temporalIndexNode = null;

        try {
            temporalIndexNode = objectMapper.readValue(content, TemporalIndexNode.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return temporalIndexNode;
    }

    public static String generateInternalNodeFilename(String blockId) {

        return "internal." + blockId + ".node";
    }

    public static String generateLeafTemporalNodeFilename(String blockId) {
        return "leaf." + blockId + ".temporal.node";
    }

    public static String generateLeafSpatialNodeFilename(String blockId) {
        return "leaf." + blockId + ".spatial.node";
    }
}
