package com.rogerguo.test.index.recovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rogerguo.test.index.*;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.index.util.IndexSerializationUtil;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.driver.PersistenceDriver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * rebuild an index tree based on checkpoint
 *
 * @author yangguo
 * @create 2022-05-24 2:22 PM
 **/
public class IndexTreeBuilder {

    public static SpatialTemporalTree rebuildIndexTree(PersistenceDriver persistenceDriver, IndexConfiguration configuration) {
        // get leaf nodes that have been checkout
        String checkpointObjectKey = LeafNodeStatusRecorder.generateCheckObjectKey(persistenceDriver.getRootUri());

        String checkpointValue = persistenceDriver.getDataAsString(checkpointObjectKey);
        List<String> checkpointObjectKeys = null;
        try {
            checkpointObjectKeys = LeafNodeStatusRecorder.deserializeCheckpoint(checkpointValue);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String filePathPrefix = persistenceDriver.getRootUri();
        int nodeCount = -1;
        List<String> nodeKeyList = new ArrayList<>();
        List<LeafNode> leafNodeList = new ArrayList<>();
        for (String key : checkpointObjectKeys) {

            String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(key);
            String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(key);
            String temporalKey = filePathPrefix + File.separator + filenameTemporal;
            String spatialKey = filePathPrefix + File.separator + filenameSpatial;

            nodeKeyList.add(temporalKey);
            nodeKeyList.add(spatialKey);

        }
        Map<String, String> nodeDataList = persistenceDriver.getDataAsStringBatch(nodeKeyList);


        for (String key : checkpointObjectKeys) {

            String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(key);
            String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(key);
            String temporalKey = filePathPrefix + File.separator + filenameTemporal;
            String spatialKey = filePathPrefix + File.separator + filenameSpatial;

            String leafTemporalNodeContent = nodeDataList.get(temporalKey);
            String leafSpatialNodeContent = nodeDataList.get(spatialKey);

            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);

            LeafNode leafNode = new LeafNode();
            leafNode.setTemporalIndexNode(temporalIndexNode);
            leafNode.setSpatialIndexNode(spatialIndexNode);
            leafNode.setBlockId(key);

            leafNodeList.add(leafNode);

            if (Integer.parseInt(key) > nodeCount) {
                nodeCount = Integer.parseInt(key);
            }
        }


        // init the index tree structure
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree();
        spatialTemporalTree.setIndexConfiguration(configuration);
        spatialTemporalTree.setBlockSize(configuration.getBlockSize());
        spatialTemporalTree.setNodeCount(nodeCount);
        spatialTemporalTree.setHeight(0);

        // rebuild
        rebuild(spatialTemporalTree, leafNodeList);

        // set leafNodeStatusRecorder
        LeafNodeStatusRecorder recorder = null;
        if (persistenceDriver instanceof ObjectStoreDriver) {
            recorder = new LeafNodeStatusRecorder((ObjectStoreDriver) persistenceDriver);
        } else if (persistenceDriver instanceof DiskDriver) {
            recorder = new LeafNodeStatusRecorder((DiskDriver) persistenceDriver);
        }
        if (recorder != null) {
            recorder.setCheckpointNodeIdList(checkpointObjectKeys);
        }
        spatialTemporalTree.setLeafNodeStatusRecorder(recorder);

        return spatialTemporalTree;

    }

    @Deprecated
    public static SpatialTemporalTree rebuildIndexTree(IndexConfiguration configuration) {


        // get leaf nodes that have been checkout
        ObjectStoreDriver objectStoreDriver = new ObjectStoreDriver(configuration.getBucketNameInS3(), configuration.getRegionS3(), configuration.getRootDirnameInBucket());
        String checkpointObjectKey = LeafNodeStatusRecorder.generateCheckObjectKey(configuration.getRootDirnameInBucket());

        String checkpointValue = objectStoreDriver.getDataAsString(checkpointObjectKey);
        List<String> checkpointObjectKeys = null;
        try {
            checkpointObjectKeys = LeafNodeStatusRecorder.deserializeCheckpoint(checkpointValue);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String filePathPrefix = objectStoreDriver.getRootUri();
        int nodeCount = -1;
        List<String> nodeKeyList = new ArrayList<>();
        List<LeafNode> leafNodeList = new ArrayList<>();
        for (String key : checkpointObjectKeys) {

            String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(key);
            String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(key);
            String temporalKey = filePathPrefix + File.separator + filenameTemporal;
            String spatialKey = filePathPrefix + File.separator + filenameSpatial;

            nodeKeyList.add(temporalKey);
            nodeKeyList.add(spatialKey);
            /*String leafTemporalNodeContent = objectStoreDriver.getDataAsString(temporalKey);
            String leafSpatialNodeContent = objectStoreDriver.getDataAsString(spatialKey);

            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);

            LeafNode leafNode = new LeafNode();
            leafNode.setTemporalIndexNode(temporalIndexNode);
            leafNode.setSpatialIndexNode(spatialIndexNode);
            leafNode.setBlockId(key);

            leafNodeList.add(leafNode);

            if (Integer.parseInt(key) > nodeCount) {
                nodeCount = Integer.parseInt(key);
            }*/
        }
        Map<String, String> nodeDataList = objectStoreDriver.getDataAsStringBatch(nodeKeyList);
        for (String key : checkpointObjectKeys) {

            String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(key);
            String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(key);
            String temporalKey = filePathPrefix + File.separator + filenameTemporal;
            String spatialKey = filePathPrefix + File.separator + filenameSpatial;

            nodeKeyList.add(temporalKey);
            nodeKeyList.add(spatialKey);
            String leafTemporalNodeContent = nodeDataList.get(temporalKey);
            String leafSpatialNodeContent = nodeDataList.get(spatialKey);

            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);

            LeafNode leafNode = new LeafNode();
            leafNode.setTemporalIndexNode(temporalIndexNode);
            leafNode.setSpatialIndexNode(spatialIndexNode);
            leafNode.setBlockId(key);

            leafNodeList.add(leafNode);

            if (Integer.parseInt(key) > nodeCount) {
                nodeCount = Integer.parseInt(key);
            }
        }


        // init the index tree structure
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree();
        spatialTemporalTree.setIndexConfiguration(configuration);
        spatialTemporalTree.setBlockSize(configuration.getBlockSize());
        spatialTemporalTree.setNodeCount(nodeCount);
        spatialTemporalTree.setHeight(0);

        // rebuild
        rebuild(spatialTemporalTree, leafNodeList);

        // set leafNodeStatusRecorder
        LeafNodeStatusRecorder recorder = new LeafNodeStatusRecorder(configuration);
        recorder.setCheckpointNodeIdList(checkpointObjectKeys);
        spatialTemporalTree.setLeafNodeStatusRecorder(recorder);

        return spatialTemporalTree;
    }

    private static void rebuild(SpatialTemporalTree tree, List<LeafNode> leafNodeList) {
        // rebuild tree according to leaf nodes
        int blockSize = tree.getBlockSize();  // the number of tuples in each node


        List<InternalNode> internalNodeList = new ArrayList<>();
        for (int i = 0; i < leafNodeList.size(); i = i + blockSize) {
            List<LeafNode> subList = leafNodeList.subList(i, Math.min(i + blockSize, leafNodeList.size()));
            InternalNode internalNode = new InternalNode();
            internalNode.setTuples(new ArrayList<>());
            internalNode.setIndexTree(tree);
            internalNode.setBlockId(String.valueOf(tree.generateBlockId()));
            for (LeafNode leafNode : subList) {
                leafNode.setParentNode(internalNode);
                leafNode.setIndexTree(tree);
                leafNode.setFlushed(true);
                InternalNodeTuple internalNodeTuple = new InternalNodeTuple(leafNode.getBlockId(), leafNode.getTemporalIndexNode().calculateStartTimestamp(), leafNode.getTemporalIndexNode().calculateStopTimestamp(), leafNode);
                internalNodeTuple.setNodeType(NodeType.LEAF);
                internalNode.getTuples().add(internalNodeTuple);
            }
            internalNodeList.add(internalNode);
        }

        rebuildInternalNode(tree, internalNodeList);

        // create a new active leaf node
        LeafNode leafNode = new LeafNode(internalNodeList.get(internalNodeList.size() - 1), tree);
        tree.setActiveNode(leafNode);
    }

    private static void rebuildInternalNode(SpatialTemporalTree tree, List<InternalNode> internalNodeList) {

        if (internalNodeList.size() <= tree.getBlockSize()) {
            // now we reach the top level (i.e., root node)
            InternalNode newInternalNode = new InternalNode();
            newInternalNode.setTuples(new ArrayList<>());
            newInternalNode.setIndexTree(tree);
            newInternalNode.setBlockId(String.valueOf(tree.generateBlockId()));
            newInternalNode.setParentNode(null);

            for (InternalNode internalNode : internalNodeList) {
                internalNode.setParentNode(newInternalNode);
                InternalNodeTuple internalNodeTuple = new InternalNodeTuple(internalNode.getBlockId(),
                                                                            internalNode.getTuples().get(0).getStartTimestamp(),
                                                                            internalNode.getTuples().get(internalNode.getTuples().size()-1).getStopTimestamp(),
                                                                            internalNode
                                                                            );
                internalNodeTuple.setNodeType(NodeType.INTERNAL);
                newInternalNode.getTuples().add(internalNodeTuple);
            }

            tree.setRoot(newInternalNode);
        } else {
            // we need to create internal nodes for input list
            List<InternalNode> generatedInternalNodeList = new ArrayList<>();
            for (int i = 0; i < internalNodeList.size(); i = i + tree.getBlockSize()) {
                List<InternalNode> subList = internalNodeList.subList(i, Math.min(i + tree.getBlockSize(), internalNodeList.size()));
                InternalNode newInternalNode = new InternalNode();
                newInternalNode.setTuples(new ArrayList<>());
                newInternalNode.setIndexTree(tree);
                newInternalNode.setBlockId(String.valueOf(tree.generateBlockId()));

                for (InternalNode internalNode : subList) {
                    internalNode.setParentNode(newInternalNode);
                    InternalNodeTuple tuple = new InternalNodeTuple(internalNode.getBlockId(),
                                                                    internalNode.getTuples().get(0).getStartTimestamp(),
                                                                    internalNode.getTuples().get(internalNode.getTuples().size()-1).getStopTimestamp(),
                                                                    internalNode);
                    tuple.setNodeType(NodeType.INTERNAL);
                    newInternalNode.getTuples().add(tuple);
                }
                generatedInternalNodeList.add(newInternalNode);
            }


            rebuildInternalNode(tree, generatedInternalNodeList);
        }

    }

}
