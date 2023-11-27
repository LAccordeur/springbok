package com.rogerguo.test.index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.rogerguo.test.index.predicate.BasicQueryPredicate;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.recovery.LeafNodeStatusRecorder;
import com.rogerguo.test.index.spatial.TwoLevelGridIndex;
import com.rogerguo.test.index.util.IndexConfiguration;
import com.rogerguo.test.index.util.IndexSerializationUtil;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.driver.ObjectStoreDriver;
import com.rogerguo.test.storage.driver.PersistenceDriver;
import com.rogerguo.test.store.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.util.*;

/**
 * @Description
 * @Date 2021/3/16 14:31
 * @Created by X1 Carbon
 */
public class SpatialTemporalTree {

    @JsonIgnore
    private TreeNode root;

    @JsonIgnore
    private LeafNode activeNode;

    private int blockSize;

    private int nodeCount;  // used to generate block id for each node

    private int height;

    @JsonIgnore
    private LeafNodeStatusRecorder leafNodeStatusRecorder;

    @JsonIgnore
    int temporalEntryNum = 0;

    @JsonIgnore
    int spatialEntryNum = 0;

    @JsonIgnore
    private IndexConfiguration indexConfiguration = new IndexConfiguration();

    @JsonIgnore
    private TwoLevelGridIndex spatialTwoLevelGridIndex;

    private NodeType rootType;  // used for rebuild process

    private String rootNodeBlockId;  // used for rebuild process

    private PersistenceDriver persistenceDriver;

    @JsonIgnore
    private SpatioTemporalTreeNodeCache treeNodeCache;

    private static Logger logger = LoggerFactory.getLogger(SpatialTemporalTree.class);

    @Deprecated
    public SpatialTemporalTree(int blockSize) {
        this.activeNode = new LeafNode(this);
        this.root = this.activeNode;
        this.blockSize = blockSize;
        this.nodeCount = 0;
        this.spatialTwoLevelGridIndex = new TwoLevelGridIndex();
        this.rootType = NodeType.LEAF;

        this.root.setBlockId(String.valueOf(generateBlockId()));
        ((LeafNode)this.root).getSpatialIndexNode().setBlockId(this.root.getBlockId());
        ((LeafNode)this.root).getTemporalIndexNode().setBlockId(this.root.getBlockId());
        this.root.setParentNode(null);
        this.root.setIndexTree(this);
        this.rootNodeBlockId = this.root.getBlockId();
    }

    public SpatialTemporalTree(IndexConfiguration indexConfiguration) {
        this.indexConfiguration = indexConfiguration;
        this.activeNode = new LeafNode(this);
        this.root = this.activeNode;
        this.blockSize = indexConfiguration.getBlockSize();
        this.nodeCount = 0;
        this.spatialTwoLevelGridIndex = new TwoLevelGridIndex();
        this.rootType = NodeType.LEAF;
        this.height = 1;

        this.root.setBlockId(String.valueOf(generateBlockId()));
        ((LeafNode)this.root).getSpatialIndexNode().setBlockId(this.root.getBlockId());
        ((LeafNode)this.root).getTemporalIndexNode().setBlockId(this.root.getBlockId());
        this.root.setParentNode(null);
        this.root.setIndexTree(this);
        this.rootNodeBlockId = this.root.getBlockId();

        this.persistenceDriver = new ObjectStoreDriver(indexConfiguration.getBucketNameInS3(), indexConfiguration.getRegionS3(), indexConfiguration.getRootDirnameInBucket());
        if (StoreConfig.IS_ENABLE_DISK_STORAGE_FOR_INDEX_NODE) {
            this.persistenceDriver = new DiskDriver("/home/ubuntu/data/index-test");
        }

        this.treeNodeCache = new SpatioTemporalTreeNodeCache(this, this.persistenceDriver);
    }

    /**
     * for in-memory test
     * @param indexConfiguration
     * @param index
     */
    @Deprecated
    public SpatialTemporalTree(IndexConfiguration indexConfiguration, TwoLevelGridIndex index) {
        this.indexConfiguration = indexConfiguration;
        this.activeNode = new LeafNode(this);
        this.root = this.activeNode;
        this.blockSize = indexConfiguration.getBlockSize();
        this.nodeCount = 0;
        this.spatialTwoLevelGridIndex = index;
        this.rootType = NodeType.LEAF;
        this.height = 1;

        this.root.setBlockId(String.valueOf(generateBlockId()));
        ((LeafNode)this.root).getSpatialIndexNode().setBlockId(this.root.getBlockId());
        ((LeafNode)this.root).getTemporalIndexNode().setBlockId(this.root.getBlockId());
        this.root.setParentNode(null);
        this.root.setIndexTree(this);
        this.rootNodeBlockId = this.root.getBlockId();
    }

    public static IndexConfiguration getDefaultIndexConfiguration() {
        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        int blockSize = 50;
        boolean isUseLazyParentUpdateForActiveNode = true;
        boolean isUsePreciseSpatialIndex = true;
        boolean isEnableSpatialIndex = true;
        IndexConfiguration indexConfiguration = new IndexConfiguration(blockSize, isUseLazyParentUpdateForActiveNode, bucketName, rootDirnameInBucket, region, isUsePreciseSpatialIndex, isEnableSpatialIndex);
        return indexConfiguration;
    }

    /**
     * only used for rebuild process
     */
    public SpatialTemporalTree() {}

    public String printStatus() {

        String status = "[SpatioTemporal Tree] status: nodeSize: " + blockSize + ", nodeCount: " + nodeCount + ", height: " + height + ", temporalEntryNum: " + temporalEntryNum + ", spatialEntryNum: " + spatialEntryNum;

        return status;
    }

    public void insert(TrajectorySegmentMeta meta) {

        if (indexConfiguration.isLazyParentUpdateForActiveNode()) {
            logger.info("insertion: using lazy mode to update parent nodes");
            activeNode.insertForLazyParentUpdate(meta);
        } else {
            logger.info("insertion: using common mode to update parent nodes");
            activeNode.insert(meta);
        }
    }

    @Deprecated
    public List<NodeTuple> search(BasicQueryPredicate predicate) {

        List<NodeTuple> resultTuples = new ArrayList<>();

        Queue<TreeNode> treeNodeQueue = new LinkedList<>();
        treeNodeQueue.add(root);

        while (!treeNodeQueue.isEmpty()) {
            TreeNode node = treeNodeQueue.poll();
            List<NodeTuple> results = node.search(predicate);
            for (NodeTuple nodeTuple : results) {
                if (nodeTuple instanceof InternalNodeTuple) {
                    InternalNodeTuple internalNodeTuple = (InternalNodeTuple) nodeTuple;
                    treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                } else {
                    resultTuples.add(nodeTuple);
                }
            }

        }
        return removeDuplicateTuples(resultTuples);
    }

    public List<NodeTuple> searchForIdTemporal(IdTemporalQueryPredicate predicate) {
        List<NodeTuple> resultTuples = new ArrayList<>();

        int searchNodeCount = 0;
        Queue<TreeNode> treeNodeQueue = new LinkedList<>();
        if (indexConfiguration.isLazyParentUpdateForActiveNode() && predicate.getStopTimestamp() >= activeNode.getStartTimeOfFirstTuple()) {
            treeNodeQueue.add(activeNode);
            searchNodeCount++;
        }
        if (!treeNodeQueue.contains(root)) {
            treeNodeQueue.add(root);
            searchNodeCount++;
        }

        while (!treeNodeQueue.isEmpty()) {
            TreeNode node = treeNodeQueue.poll();
            List<NodeTuple> results = node.searchForIdTemporal(predicate);
            for (NodeTuple nodeTuple : results) {
                if (nodeTuple instanceof InternalNodeTuple) {
                    InternalNodeTuple internalNodeTuple = (InternalNodeTuple) nodeTuple;
                    if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                        treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                        searchNodeCount++;
                    } else  {
                        if (internalNodeTuple.getNodePointer() != null) {
                            treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                            searchNodeCount++;
                        } else {
                            treeNodeQueue.offer(treeNodeCache.getTreeNodeFromCacheOrPersistenceStore(internalNodeTuple.getBlockId(), internalNodeTuple.getNodeType(), node));
                            searchNodeCount++;
                        }
                    }
                } else {
                    resultTuples.add(nodeTuple);
                }
            }

        }

        System.out.println("the number of searched index node: " + searchNodeCount);
        return removeDuplicateTuples(resultTuples);
    }

    public List<NodeTuple> searchForSpatialTemporal(SpatialTemporalRangeQueryPredicate predicate) {
        List<NodeTuple> resultTuples = new ArrayList<>();

        int searchNodeCount = 0;
        Queue<TreeNode> treeNodeQueue = new LinkedList<>();
        if (indexConfiguration.isLazyParentUpdateForActiveNode() && predicate.getStopTimestamp() >= activeNode.getStartTimeOfFirstTuple()) {
            treeNodeQueue.add(activeNode);
            searchNodeCount++;
        }
        if (!treeNodeQueue.contains(root)) {
            treeNodeQueue.add(root);
            searchNodeCount++;
        }

        Set<String> appearedBlocks = new HashSet<>();
        while (!treeNodeQueue.isEmpty()) {
            TreeNode node = treeNodeQueue.poll();
            List<NodeTuple> results = node.searchForSpatialTemporal(predicate);
            for (NodeTuple nodeTuple : results) {
                if (nodeTuple instanceof InternalNodeTuple) {
                    InternalNodeTuple internalNodeTuple = (InternalNodeTuple) nodeTuple;
                    //treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                    if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                        treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                        searchNodeCount++;
                    } else  {
                        if (internalNodeTuple.getNodePointer() != null) {
                            treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                            searchNodeCount++;
                        } else {
                            treeNodeQueue.offer(treeNodeCache.getTreeNodeFromCacheOrPersistenceStore(internalNodeTuple.getBlockId(), internalNodeTuple.getNodeType(), node));
                            searchNodeCount++;
                        }
                    }
                } else {
                    // some grids may point to the same data blocks
                    if (!appearedBlocks.contains(nodeTuple.getBlockId())) {
                        resultTuples.add(nodeTuple);
                        appearedBlocks.add(nodeTuple.getBlockId());
                    }
                }
            }

        }
        System.out.println("the number of searched index node: " + searchNodeCount);
        return removeDuplicateTuples(resultTuples);
    }

    private List<NodeTuple> removeDuplicateTuples(List<NodeTuple> tuples) {

        List<NodeTuple> resultTupleList = new ArrayList<>();
        Set<String> existedBlockSet = new HashSet<>();

        for (NodeTuple tuple : tuples) {
            if (!existedBlockSet.contains(tuple.getBlockId())) {
                resultTupleList.add(tuple);
                existedBlockSet.add(tuple.getBlockId());
            }
        }

        return resultTupleList;

    }

    public void close() {
        // flush the non-full index nodes if the cache enabled
        // flush all index nodes if the cache disabled
        if (!StoreConfig.S3_OBJECT_ACCESS_MODE.equals("batch")) {
            serializeAndFlushIndex();
        } else {
            serializeAndFlushIndexBatch();
        }

        if (StoreConfig.ENABLE_INDEX_NODE_CACHE) {
            // flush the non-flushed (full) index nodes in the cache
            this.getTreeNodeCache().clearAndFlushAll();
        }
    }

    /**
     * Serialize and flush the whole index tree, for ease to debug, each node will be stored as a seperate file
     */
    public void serializeAndFlushIndex() {
        SpatialTemporalTree tree = this;
        TreeNode rootNode = tree.getRoot();

        String filePathPrefix = persistenceDriver.getRootUri();

        // index tree meta persistence
        String indexTreeMetaString = IndexSerializationUtil.serializeIndexTreeMeta(tree);
        persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.TREE_META, indexTreeMetaString);


        if (rootNode instanceof LeafNode) {
            persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_SPATIAL_FILENAME, IndexSerializationUtil.serializeLeafSpatialNode(((LeafNode) rootNode).getSpatialIndexNode()));
            persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_TEMPORAL_FILENAME, IndexSerializationUtil.serializeLeafTemporalNode(((LeafNode) rootNode).getTemporalIndexNode()));

        }

        if (rootNode instanceof InternalNode) {
            InternalNode internalNode = (InternalNode) rootNode;
            persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_INTERNAL_FILENAME, IndexSerializationUtil.serializeInternalNode(internalNode));

            for (InternalNodeTuple internalNodeTuple : internalNode.getTuples()) {
                traverseNodes(internalNodeTuple.getNodePointer());
            }
        }

    }

    private void traverseNodes(TreeNode treeNode) {
        String filePathPrefix = persistenceDriver.getRootUri();

        if (treeNode != null) {
            if (treeNode instanceof InternalNode) {
                InternalNode treeNodeInternal = (InternalNode) treeNode;
                String internalNodeString = IndexSerializationUtil.serializeInternalNode(treeNodeInternal);
                String filenameInternal = IndexSerializationUtil.generateInternalNodeFilename(treeNodeInternal.getBlockId());
                persistenceDriver.flush(filePathPrefix + File.separator + filenameInternal, internalNodeString);

                for (InternalNodeTuple internalNodeTuple : treeNodeInternal.getTuples()) {
                    traverseNodes(internalNodeTuple.getNodePointer());
                }
            }

            if (treeNode instanceof LeafNode) {

                LeafNode treeNodeLeaf = (LeafNode) treeNode;
                String temporalIndexString = IndexSerializationUtil.serializeLeafTemporalNode(treeNodeLeaf.getTemporalIndexNode());
                String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(treeNodeLeaf.getBlockId());
                persistenceDriver.flush(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);

                String spatialIndexString = IndexSerializationUtil.serializeLeafSpatialNode(treeNodeLeaf.getSpatialIndexNode());
                String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(treeNodeLeaf.getBlockId());
                persistenceDriver.flush(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);
            }
        }
    }

    public void serializeAndFlushIndexBatch() {
        Map<String, String> keyValueMap = new HashMap<>();

        SpatialTemporalTree tree = this;
        TreeNode rootNode = tree.getRoot();

        String filePathPrefix = persistenceDriver.getRootUri();

        // index tree meta persistence
        String indexTreeMetaString = IndexSerializationUtil.serializeIndexTreeMeta(tree);
        //persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.TREE_META, indexTreeMetaString);
        keyValueMap.put(filePathPrefix + File.separator + IndexSerializationUtil.TREE_META, indexTreeMetaString);


        if (rootNode instanceof LeafNode) {
            //persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_SPATIAL_FILENAME, IndexSerializationUtil.serializeLeafSpatialNode(((LeafNode) rootNode).getSpatialIndexNode()));
            //persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_TEMPORAL_FILENAME, IndexSerializationUtil.serializeLeafTemporalNode(((LeafNode) rootNode).getTemporalIndexNode()));
            keyValueMap.put(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_SPATIAL_FILENAME, IndexSerializationUtil.serializeLeafSpatialNode(((LeafNode) rootNode).getSpatialIndexNode()));
            keyValueMap.put(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_TEMPORAL_FILENAME, IndexSerializationUtil.serializeLeafTemporalNode(((LeafNode) rootNode).getTemporalIndexNode()));

        }

        if (rootNode instanceof InternalNode) {
            InternalNode internalNode = (InternalNode) rootNode;
            //persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_INTERNAL_FILENAME, IndexSerializationUtil.serializeInternalNode(internalNode));
            keyValueMap.put(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_INTERNAL_FILENAME, IndexSerializationUtil.serializeInternalNode(internalNode));

            for (InternalNodeTuple internalNodeTuple : internalNode.getTuples()) {
                traverseNodesBatch(internalNodeTuple.getNodePointer(), keyValueMap);
            }
        }

        persistenceDriver.flushBatch(keyValueMap);

    }

    private void traverseNodesBatch(TreeNode treeNode, Map<String, String> keyValueMap) {
        String filePathPrefix = persistenceDriver.getRootUri();

        if (treeNode != null) {
            if (treeNode instanceof InternalNode) {
                InternalNode treeNodeInternal = (InternalNode) treeNode;
                String internalNodeString = IndexSerializationUtil.serializeInternalNode(treeNodeInternal);
                String filenameInternal = IndexSerializationUtil.generateInternalNodeFilename(treeNodeInternal.getBlockId());
                //persistenceDriver.flush(filePathPrefix + File.separator + filenameInternal, internalNodeString);
                keyValueMap.put(filePathPrefix + File.separator + filenameInternal, internalNodeString);

                for (InternalNodeTuple internalNodeTuple : treeNodeInternal.getTuples()) {
                    traverseNodesBatch(internalNodeTuple.getNodePointer(), keyValueMap);
                }
            }

            if (treeNode instanceof LeafNode) {

                LeafNode treeNodeLeaf = (LeafNode) treeNode;
                String temporalIndexString = IndexSerializationUtil.serializeLeafTemporalNode(treeNodeLeaf.getTemporalIndexNode());
                String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(treeNodeLeaf.getBlockId());
                //persistenceDriver.flush(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);
                keyValueMap.put(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);

                String spatialIndexString = IndexSerializationUtil.serializeLeafSpatialNode(treeNodeLeaf.getSpatialIndexNode());
                String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(treeNodeLeaf.getBlockId());
                //persistenceDriver.flush(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);
                keyValueMap.put(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);
            }
        }
    }

    /**
     * load the whole index tree from the file.
     * (1) when the node cache is enabled, only a few active nodes are loaded
     * (2) when the node cache is disabled, we load all nodes
     *  filename the filename of index tree meta
     * @return
     */
    public SpatialTemporalTree loadAndRebuildIndex() {

        String filename = IndexSerializationUtil.TREE_META;  // filename the filename of index tree meta

        String filePathPrefix = persistenceDriver.getRootUri();

        String indexTreeMetaFilename = filePathPrefix + File.separator + filename;
        String indexTreeMetaContent = persistenceDriver.getDataAsString(indexTreeMetaFilename);
        SpatialTemporalTree spatialTemporalTree = IndexSerializationUtil.deserializeIndexTreeMeta(indexTreeMetaContent);
        spatialTemporalTree.persistenceDriver = this.persistenceDriver;
        if (StoreConfig.ENABLE_INDEX_NODE_CACHE) {
            spatialTemporalTree.treeNodeCache = new SpatioTemporalTreeNodeCache(spatialTemporalTree, spatialTemporalTree.persistenceDriver);
        }


        Map<String, String> nodeDataMap = null;
        if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
            List<String> indexNodeKeyList = persistenceDriver.listKeysWithSamePrefix(filePathPrefix);
            nodeDataMap = persistenceDriver.getDataAsStringBatch(indexNodeKeyList);
        }

        if (spatialTemporalTree.getRootType() == NodeType.LEAF) {
            // we only have one index node
            String leafTemporalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.ROOT_TEMPORAL_FILENAME;
            //String leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
            String leafTemporalNodeContent = null;
            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                leafTemporalNodeContent = nodeDataMap.get(leafTemporalNodeFilename);
            } else {
                leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
            }

            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            String leafSpatialNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.ROOT_SPATIAL_FILENAME;
            //String leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);
            String leafSpatialNodeContent = null;
            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                leafSpatialNodeContent = nodeDataMap.get(leafSpatialNodeFilename);
            } else {
                leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);
            }
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);

            LeafNode rootNode = new LeafNode();
            rootNode.setParentNode(null);
            rootNode.setIndexTree(spatialTemporalTree);
            rootNode.setBlockId(spatialTemporalTree.getRootNodeBlockId());
            rootNode.setTemporalIndexNode(temporalIndexNode);
            rootNode.setSpatialIndexNode(spatialIndexNode);

            temporalIndexNode.setParentNode(null);
            temporalIndexNode.setIndexTree(spatialTemporalTree);

            spatialIndexNode.setParentNode(null);
            spatialIndexNode.setIndexTree(spatialTemporalTree);

            spatialTemporalTree.setRoot(rootNode);
            // finally, we set the active node
            spatialTemporalTree.setActiveNode(rootNode);

        }

        if (spatialTemporalTree.getRootType() == NodeType.INTERNAL) {
            String rootInternalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.ROOT_INTERNAL_FILENAME;
            //String rootInternalNodeContent = persistenceDriver.getDataAsString(rootInternalNodeFilename);
            String rootInternalNodeContent = null;
            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                rootInternalNodeContent = nodeDataMap.get(rootInternalNodeFilename);
            } else {
                rootInternalNodeContent = persistenceDriver.getDataAsString(rootInternalNodeFilename);
            }
            InternalNode rootNode = IndexSerializationUtil.deserializeInternalNode(rootInternalNodeContent);
            rootNode.setParentNode(null);
            rootNode.setIndexTree(spatialTemporalTree);
            rootNode.setBlockId(spatialTemporalTree.getRootNodeBlockId());

            spatialTemporalTree.setRoot(rootNode);

            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                for (InternalNodeTuple internalNodeTuple : rootNode.getTuples()) {
                    traverseAndRebuildNodes(internalNodeTuple, rootNode, spatialTemporalTree, nodeDataMap);
                }
            } else {
                for (int i = 0; i < rootNode.getTuples().size(); i++) {
                    if (i == rootNode.getTuples().size() - 1) {
                        InternalNodeTuple tupleLatest = rootNode.getTuples().get(i);
                        traverseAndRebuildNodes(tupleLatest, rootNode, spatialTemporalTree, nodeDataMap);
                    }
                }
            }
        }

        nodeDataMap.clear();
        nodeDataMap = null;
        return spatialTemporalTree;
    }


    private void traverseAndRebuildNodes(NodeTuple nodeTuple, TreeNode parentNode, SpatialTemporalTree indexTree, Map<String, String> nodeDataMap) {
        String filePathPrefix = persistenceDriver.getRootUri();

        if (nodeTuple.getNodeType() == NodeType.INTERNAL) {
            // rebuild the child node pointed by this tuple
            String nodeBlockId = nodeTuple.getBlockId();
            String internalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateInternalNodeFilename(nodeBlockId);
            //String internalNodeContent = persistenceDriver.getDataAsString(internalNodeFilename);
            String internalNodeContent = null;
            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                internalNodeContent = nodeDataMap.get(internalNodeFilename);
            } else {
                internalNodeContent = persistenceDriver.getDataAsString(internalNodeFilename);
            }
            InternalNode internalNode = IndexSerializationUtil.deserializeInternalNode(internalNodeContent);
            ((InternalNodeTuple) nodeTuple).setNodePointer(internalNode);
            internalNode.setParentNode(parentNode);
            internalNode.setIndexTree(indexTree);

            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                for (InternalNodeTuple internalNodeTuple : internalNode.getTuples()) {
                    traverseAndRebuildNodes(internalNodeTuple, internalNode, indexTree, nodeDataMap);
                }
            } else {
                for (int i = 0; i < internalNode.getTuples().size(); i++) {
                    if (i == internalNode.getTuples().size() - 1) {
                        InternalNodeTuple tupleLatest = internalNode.getTuples().get(i);
                        traverseAndRebuildNodes(tupleLatest, internalNode, indexTree, nodeDataMap);
                    }
                }
            }
        }

        if (nodeTuple.getNodeType() == NodeType.LEAF) {
            String nodeBlockId = nodeTuple.getBlockId();
            String leafTemporalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateLeafTemporalNodeFilename(nodeBlockId);
            //String leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
            String leafTemporalNodeContent = null;
            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                leafTemporalNodeContent = nodeDataMap.get(leafTemporalNodeFilename);
            } else {
                leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
            }
            String leafSpatialNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateLeafSpatialNodeFilename(nodeBlockId);
            //String leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);
            String leafSpatialNodeContent = null;
            if (!StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                leafSpatialNodeContent = nodeDataMap.get(leafSpatialNodeFilename);
            } else {
                leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);
            }

            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            temporalIndexNode.setIndexTree(indexTree);
            temporalIndexNode.setParentNode(parentNode);
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);
            spatialIndexNode.setIndexTree(indexTree);
            spatialIndexNode.setParentNode(parentNode);

            LeafNode leafNode = new LeafNode();
            leafNode.setTemporalIndexNode(temporalIndexNode);
            leafNode.setSpatialIndexNode(spatialIndexNode);
            leafNode.setBlockId(nodeBlockId);
            leafNode.setIndexTree(indexTree);
            leafNode.setParentNode(parentNode);

            ((InternalNodeTuple)nodeTuple).setNodePointer(leafNode);

            // set active node (only last call needed)
            indexTree.setActiveNode(leafNode);

        }


    }

    public LeafNodeStatusRecorder getLeafNodeStatusRecorder() {
        return leafNodeStatusRecorder;
    }

    public void setLeafNodeStatusRecorder(LeafNodeStatusRecorder leafNodeStatusRecorder) {
        this.leafNodeStatusRecorder = leafNodeStatusRecorder;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int generateBlockId() {
        return nodeCount++;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public TreeNode getRoot() {
        return root;
    }

    public void setRoot(TreeNode root) {
        this.root = root;
    }

    public TreeNode getActiveNode() {
        return activeNode;
    }

    public void setActiveNode(LeafNode activeNode) {
        this.activeNode = activeNode;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setSpatialTwoLevelGridIndex(TwoLevelGridIndex spatialTwoLevelGridIndex) {
        this.spatialTwoLevelGridIndex = spatialTwoLevelGridIndex;
    }

    public NodeType getRootType() {
        return rootType;
    }

    public void setRootType(NodeType rootType) {
        this.rootType = rootType;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public String getRootNodeBlockId() {
        return rootNodeBlockId;
    }

    public void setRootNodeBlockId(String rootNodeBlockId) {
        this.rootNodeBlockId = rootNodeBlockId;
    }

    public void setIndexConfiguration(IndexConfiguration indexConfiguration) {
        this.indexConfiguration = indexConfiguration;
    }

    @Override
    public String toString() {
        return "nodeCount=" + (nodeCount+1) + "\n\nroot=" + root;

    }

    public TwoLevelGridIndex getSpatialTwoLevelGridIndex() {
        return spatialTwoLevelGridIndex;
    }

    public IndexConfiguration getIndexConfiguration() {
        return indexConfiguration;
    }

    public SpatioTemporalTreeNodeCache getTreeNodeCache() {
        return treeNodeCache;
    }

    public void setTreeNodeCache(SpatioTemporalTreeNodeCache treeNodeCache) {
        this.treeNodeCache = treeNodeCache;
    }
}
