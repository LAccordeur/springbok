package com.rogerguo.test.index;

import com.rogerguo.test.index.util.IndexSerializationUtil;
import com.rogerguo.test.storage.driver.PersistenceDriver;
import com.rogerguo.test.store.StoreConfig;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;

/**
 * an index cache for spatial temporal tree. the cache is only for the full nodes
 * for a large data volume, the index size can be large, so in the design, all full nodes will be put into this cache first.
 * Then, when the cache size exceeds a threshold, we will flush a (old) part of nodes to the object store
 * for an index query, we first check the cache and then fecth node from S3 if not available
 */

public class SpatioTemporalTreeNodeCache {

    private SpatialTemporalTree spatialTemporalTree;

    private int cacheSize = StoreConfig.NODE_CACHE_SIZE;

    private Map<String, TreeNode> treeNodeCacheMap = new HashMap<>();   // key is the node id

    private Queue<String> treeNodeCacheQueue = new LinkedList<>();  // used to kick out tree node

    private PersistenceDriver persistenceDriver;

    public SpatioTemporalTreeNodeCache(SpatialTemporalTree tree, PersistenceDriver persistenceDriver) {
        this.persistenceDriver = persistenceDriver;
        this.spatialTemporalTree = tree;
    }

    public void addTreeNodeToCache(TreeNode treeNode) {


        treeNodeCacheMap.put(treeNode.getBlockId(), treeNode);
        treeNodeCacheQueue.offer(treeNode.getBlockId());

        if (treeNodeCacheMap.size() >= cacheSize) {
            kickAndFlushTreeNode();
        }
    }

    public TreeNode getTreeNodeFromCacheOrPersistenceStore(String nodeId, NodeType nodeType, TreeNode parentNode) {

        if (treeNodeCacheMap.containsKey(nodeId)) {
            //System.out.println("get node from index node cache");
            return treeNodeCacheMap.get(nodeId);
        } else {
            //System.out.println("get node from S3");

            String filePathPrefix = persistenceDriver.getRootUri();
            if (nodeType == NodeType.INTERNAL) {
                String internalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateInternalNodeFilename(nodeId);
                String internalNodeContent = persistenceDriver.getDataAsString(internalNodeFilename);
                InternalNode internalNode = IndexSerializationUtil.deserializeInternalNode(internalNodeContent);
                internalNode.setBlockId(nodeId);
                internalNode.setIndexTree(this.spatialTemporalTree);
                internalNode.setParentNode(parentNode);
                internalNode.setFlushed(true);

                treeNodeCacheMap.put(internalNode.getBlockId(), internalNode);
                treeNodeCacheQueue.offer(internalNode.getBlockId());

                if (treeNodeCacheMap.size() > cacheSize) {
                    kickAndFlushTreeNode();
                }

                return internalNode;
            } else {
                // leaf node
                String leafTemporalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateLeafTemporalNodeFilename(nodeId);
                String leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
                String leafSpatialNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateLeafSpatialNodeFilename(nodeId);
                String leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);

                TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
                SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);
                spatialIndexNode.setIndexTree(this.spatialTemporalTree);
                temporalIndexNode.setIndexTree(this.spatialTemporalTree);
                spatialIndexNode.setParentNode(parentNode);
                temporalIndexNode.setParentNode(parentNode);
                spatialIndexNode.setFlushed(true);
                temporalIndexNode.setFlushed(true);

                LeafNode leafNode = new LeafNode();
                leafNode.setTemporalIndexNode(temporalIndexNode);
                leafNode.setSpatialIndexNode(spatialIndexNode);
                leafNode.setBlockId(nodeId);
                leafNode.setIndexTree(this.spatialTemporalTree);
                leafNode.setParentNode(parentNode);
                leafNode.setFlushed(true);

                treeNodeCacheMap.put(leafNode.getBlockId(), leafNode);
                treeNodeCacheQueue.offer(leafNode.getBlockId());

                if (treeNodeCacheMap.size() > cacheSize) {
                    kickAndFlushTreeNode();
                }

                return leafNode;

            }
        }
    }

    /**
     * kick off the 1/3 nodes from cache, flush those nodes without persistence
     */
    private void kickAndFlushTreeNode() {
        String filePathPrefix = persistenceDriver.getRootUri();
        Map<String, String> keyValueMap = new HashMap<>();
        int kickCount = 0;
        while(!treeNodeCacheQueue.isEmpty()) {
            String nodeId = treeNodeCacheQueue.poll();
            TreeNode treeNode = treeNodeCacheMap.get(nodeId);
            if (!treeNode.isFlushed()) {
                if (treeNode instanceof InternalNode) {
                    InternalNode treeNodeInternal = (InternalNode) treeNode;
                    String internalNodeString = IndexSerializationUtil.serializeInternalNode(treeNodeInternal);
                    String filenameInternal = IndexSerializationUtil.generateInternalNodeFilename(nodeId);
                    keyValueMap.put(filePathPrefix + File.separator + filenameInternal, internalNodeString);
                }

                if (treeNode instanceof LeafNode) {
                    LeafNode treeNodeLeaf = (LeafNode) treeNode;
                    String temporalIndexString = IndexSerializationUtil.serializeLeafTemporalNode(treeNodeLeaf.getTemporalIndexNode());
                    String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(nodeId);
                    keyValueMap.put(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);

                    String spatialIndexString = IndexSerializationUtil.serializeLeafSpatialNode(treeNodeLeaf.getSpatialIndexNode());
                    String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(nodeId);
                    keyValueMap.put(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);
                }
                treeNode.setFlushed(true);
            }

            treeNodeCacheMap.remove(nodeId);
            kickCount++;
            if (kickCount >= cacheSize / 3.0) {
                break;
            }
        }
        persistenceDriver.flushBatch(keyValueMap);
        System.out.println("multiple index nodes are flushed: " + keyValueMap.keySet());

    }

    public void clearAndFlushAll() {
        String filePathPrefix = persistenceDriver.getRootUri();

        Map<String, String> keyValueMap = new HashMap<>();
        while (!treeNodeCacheQueue.isEmpty()) {
            String nodeId = treeNodeCacheQueue.poll();
            TreeNode treeNode = treeNodeCacheMap.get(nodeId);
            if (!treeNode.isFlushed()) {
                if (treeNode instanceof InternalNode) {
                    InternalNode treeNodeInternal = (InternalNode) treeNode;
                    String internalNodeString = IndexSerializationUtil.serializeInternalNode(treeNodeInternal);
                    String filenameInternal = IndexSerializationUtil.generateInternalNodeFilename(nodeId);
                    keyValueMap.put(filePathPrefix + File.separator + filenameInternal, internalNodeString);
                }

                if (treeNode instanceof LeafNode) {
                    LeafNode treeNodeLeaf = (LeafNode) treeNode;
                    String temporalIndexString = IndexSerializationUtil.serializeLeafTemporalNode(treeNodeLeaf.getTemporalIndexNode());
                    String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(nodeId);
                    keyValueMap.put(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);

                    String spatialIndexString = IndexSerializationUtil.serializeLeafSpatialNode(treeNodeLeaf.getSpatialIndexNode());
                    String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(nodeId);
                    keyValueMap.put(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);
                }
                treeNode.setFlushed(true);
            }
        }
        persistenceDriver.flushBatch(keyValueMap);
        treeNodeCacheMap.clear();
    }

}
