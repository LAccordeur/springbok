package com.rogerguo.test.index;

import com.rogerguo.test.index.predicate.BasicQueryPredicate;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.store.StoreConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/3/15 15:16
 * @Created by Yang GUO
 */
public class LeafNode extends TreeNode {

    private TemporalIndexNode temporalIndexNode;

    private SpatialIndexNode spatialIndexNode;

    public LeafNode(SpatialTemporalTree indexTree) {
        // only used for root node
        this.temporalIndexNode = new TemporalIndexNode(null, indexTree);
        this.spatialIndexNode = new SpatialIndexNode(null, indexTree);
    }

    // used for rebuild tree
    public LeafNode() {}

    public LeafNode(TreeNode parentNode, SpatialTemporalTree indexTree) {
        super(parentNode, indexTree);
        this.temporalIndexNode = new TemporalIndexNode(parentNode, indexTree);
        this.spatialIndexNode = new SpatialIndexNode(parentNode, indexTree);
        this.setBlockId(String.valueOf(this.getIndexTree().generateBlockId()));
        this.temporalIndexNode.setBlockId(this.getBlockId());
        this.spatialIndexNode.setBlockId(this.getBlockId());
    }

    public List<NodeTuple> search(BasicQueryPredicate meta) {
        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();

        if (meta instanceof IdTemporalQueryPredicate) {
            List<NodeTuple> temporalResultTuples = this.temporalIndexNode.search(meta);
            resultTuples.addAll(temporalResultTuples);
        }

        if (meta instanceof SpatialTemporalRangeQueryPredicate) {
            List<NodeTuple> spatialTemporalTuples = this.spatialIndexNode.search(meta);
            resultTuples.addAll(spatialTemporalTuples);
        }

        return resultTuples;
    }

    @Override
    List<NodeTuple> searchForIdTemporal(IdTemporalQueryPredicate predicate) {
        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();

        List<NodeTuple> temporalResultTuples = this.temporalIndexNode.search(predicate);
        resultTuples.addAll(temporalResultTuples);

        return resultTuples;
    }

    @Override
    List<NodeTuple> searchForSpatialTemporal(SpatialTemporalRangeQueryPredicate predicate) {
        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();

        List<NodeTuple> spatialTemporalTuples = this.spatialIndexNode.search(predicate);
        resultTuples.addAll(spatialTemporalTuples);


        return resultTuples;
    }

    public boolean insert(TrajectorySegmentMeta meta) {

        if (temporalIndexNode.getTuples().size() < this.getIndexTree().getBlockSize()) {
            temporalIndexNode.insert(meta);
            if (this.getIndexTree().getIndexConfiguration().isEnableSpatialIndex()) {
                spatialIndexNode.insert(meta);
            }

            // update the meta data in the internal node (when creating new leaf, we do not need this)
            updateMetaInInternalNode(meta);

        } else {
            if (this.getIndexTree().getIndexConfiguration().isEnableRecorderForRecovery()) {
                this.getIndexTree().getLeafNodeStatusRecorder().addFullLeafNode(this);  // add full leaf node to the recorder
            }
            // this leaf node is full, we create a new one
            LeafNode newLeafNode = new LeafNode(this.getParentNode(), this.getIndexTree());

            newLeafNode.temporalIndexNode.insert(meta);
            if (this.getIndexTree().getIndexConfiguration().isEnableSpatialIndex()) {
                newLeafNode.spatialIndexNode.insert(meta);
            }
            this.getIndexTree().setActiveNode(newLeafNode);

            // get internal tuple for this node and insert into parent node
            NodeTuple internalNodeTupleNewLeaf = generateInternalNodeTuple(newLeafNode);
            InternalNodeTuple internalNodeTupleThisLeaf = generateInternalNodeTuple(this);

            if (this.getParentNode() != null) {
                // if parentNode is full, we need to create a new parent node and update the parent node of this new leafNode
                this.getParentNode().insert(internalNodeTupleNewLeaf);
            } else {

                // if parent node is null, it means this node is root node
                TreeNode newRootNode = new InternalNode(null, this.getIndexTree());

                //NodeTuple internalNodeTupleThisLeaf = generateInternalNodeTuple(this);
                newRootNode.insert(internalNodeTupleThisLeaf);
                newRootNode.insert(internalNodeTupleNewLeaf);
                this.setParentNode(newRootNode);
                if (this instanceof LeafNode) {
                    this.getTemporalIndexNode().setParentNode(newRootNode);
                    this.getSpatialIndexNode().setParentNode(newRootNode);
                }

                newLeafNode.setParentNode(newRootNode);
                newLeafNode.getSpatialIndexNode().setParentNode(newRootNode);
                newLeafNode.getTemporalIndexNode().setParentNode(newRootNode);

                this.getIndexTree().setRoot(newRootNode);
                this.getIndexTree().setActiveNode(newLeafNode);
                this.getIndexTree().setRootNodeBlockId(newRootNode.getBlockId());
                this.getIndexTree().setRootType(NodeType.INTERNAL);
                this.getIndexTree().setHeight(this.getIndexTree().getHeight()+1);
            }

            if (StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                // we move the full node to the cache
                // for the branch of (this.getParentNode() == null)
                internalNodeTupleThisLeaf.setNodePointer(null);
                // for the branch of (this.getParentNode() != null)
                if (this.getParentNode() != null) {
                    InternalNode internalNodeParent = (InternalNode) this.getParentNode();
                    for (InternalNodeTuple tuple : internalNodeParent.getTuples()) {
                        if (tuple.getBlockId().equals(this.getBlockId())) {
                            tuple.setNodePointer(null);
                        }
                    }
                }

                this.getIndexTree().getTreeNodeCache().addTreeNodeToCache(this);
            }
        }


        return true;
    }

    /**
     * this version only attach leaf node to index tree until is full,
     * so we do not need to update its status in the parent node whenever these is a new tuple coming in
     * @param meta
     * @return
     */
    public boolean insertForLazyParentUpdate(TrajectorySegmentMeta meta) {
        if (temporalIndexNode.getTuples().size() < this.getIndexTree().getBlockSize()) {
            temporalIndexNode.insert(meta);
            if (this.getIndexTree().getIndexConfiguration().isEnableSpatialIndex()) {
                spatialIndexNode.insert(meta);
            }

        } else {
            if (this.getIndexTree().getIndexConfiguration().isEnableRecorderForRecovery()) {
                this.getIndexTree().getLeafNodeStatusRecorder().addFullLeafNode(this);  // add full leaf node to the recorder
            }
            // this leaf node is full, we create a new one
            LeafNode newLeafNode = new LeafNode(this.getParentNode(), this.getIndexTree());

            newLeafNode.temporalIndexNode.insert(meta);
            if (this.getIndexTree().getIndexConfiguration().isEnableSpatialIndex()) {
                newLeafNode.spatialIndexNode.insert(meta);
            }
            this.getIndexTree().setActiveNode(newLeafNode);

            // get internal tuple for this node and insert into parent node
            NodeTuple internalNodeTupleNewLeaf = generateInternalNodeTuple(newLeafNode);
            InternalNodeTuple internalNodeTupleThisLeaf = generateInternalNodeTuple(this);

            if (this.getParentNode() != null) {
                // lazy update meta for current node
                updateMetaInInternalNode(this, internalNodeTupleThisLeaf);

                // if parentNode is full, we need to create a new parent node and update the parent node of this new leafNode
                this.getParentNode().insert(internalNodeTupleNewLeaf);
            } else {

                // if parent node is null, it means this node is root node
                TreeNode newRootNode = new InternalNode(null, this.getIndexTree());
                newRootNode.insert(internalNodeTupleThisLeaf);
                newRootNode.insert(internalNodeTupleNewLeaf);

                this.setParentNode(newRootNode);
                if (this instanceof LeafNode) {
                    this.getTemporalIndexNode().setParentNode(newRootNode);
                    this.getSpatialIndexNode().setParentNode(newRootNode);
                }

                newLeafNode.setParentNode(newRootNode);
                newLeafNode.getSpatialIndexNode().setParentNode(newRootNode);
                newLeafNode.getTemporalIndexNode().setParentNode(newRootNode);

                this.getIndexTree().setRoot(newRootNode);
                this.getIndexTree().setActiveNode(newLeafNode);
                this.getIndexTree().setRootNodeBlockId(newRootNode.getBlockId());
                this.getIndexTree().setRootType(NodeType.INTERNAL);
                this.getIndexTree().setHeight(this.getIndexTree().getHeight()+1);
            }

            if (StoreConfig.ENABLE_INDEX_NODE_CACHE) {
                // we move the full node to the cache
                // for the branch of (this.getParentNode() == null)
                internalNodeTupleThisLeaf.setNodePointer(null);
                // for the branch of (this.getParentNode() != null)
                if (this.getParentNode() != null) {
                    InternalNode internalNodeParent = (InternalNode) this.getParentNode();
                    for (InternalNodeTuple tuple : internalNodeParent.getTuples()) {
                        if (tuple.getBlockId().equals(this.getBlockId())) {
                            tuple.setNodePointer(null);
                        }
                    }
                 }

                this.getIndexTree().getTreeNodeCache().addTreeNodeToCache(this);
            }
        }


        return true;
    }

    public long getStartTimeOfFirstTuple() {
        return temporalIndexNode.getTuples().get(0).getStartTimestamp();
    }

    private void updateMetaInInternalNode(TrajectorySegmentMeta meta) {
        TreeNode currentNode = this;
        TreeNode parentNode = currentNode.getParentNode();
        while (parentNode != null) {

            InternalNode internalNode = (InternalNode) parentNode;
            for (InternalNodeTuple tuple : internalNode.getTuples()) {
                if (tuple.getBlockId().equals(currentNode.getBlockId())) {
                    if (tuple.getStopTimestamp() < meta.getStopTimestamp()) {
                        tuple.setStopTimestamp(meta.getStopTimestamp());
                    }
                    if (tuple.getStartTimestamp() > meta.getStartTimestamp()) {
                        tuple.setStartTimestamp(meta.getStartTimestamp());
                    }
                }
            }
            currentNode = parentNode;
            parentNode = parentNode.getParentNode();
        }
    }

    private void updateMetaInInternalNode(TreeNode currentNode, InternalNodeTuple insertedTuple) {
        TreeNode parentNode = currentNode.getParentNode();
        while (parentNode != null) {

            InternalNode internalNode = (InternalNode) parentNode;
            for (InternalNodeTuple tuple : internalNode.getTuples()) {
                if (tuple.getBlockId().equals(currentNode.getBlockId())) {
                    if (tuple.getStopTimestamp() < insertedTuple.getStopTimestamp()) {
                        tuple.setStopTimestamp(insertedTuple.getStopTimestamp());
                    }
                    if (tuple.getStartTimestamp() > insertedTuple.getStartTimestamp()) {
                        tuple.setStartTimestamp(insertedTuple.getStartTimestamp());
                    }
                }
            }
            currentNode = parentNode;
            parentNode = parentNode.getParentNode();
        }
    }

    private InternalNodeTuple generateInternalNodeTuple(LeafNode node) {
        InternalNodeTuple internalNodeTuple = new InternalNodeTuple(node.getBlockId(), node.temporalIndexNode.calculateStartTimestamp(), node.temporalIndexNode.calculateStopTimestamp(), node);
        internalNodeTuple.setNodeType(NodeType.LEAF);
        return internalNodeTuple;
    }


    public boolean insert(NodeTuple tuple) {
        return true;
    }

    public TemporalIndexNode getTemporalIndexNode() {
        return temporalIndexNode;
    }

    public SpatialIndexNode getSpatialIndexNode() {
        return spatialIndexNode;
    }

    public void setTemporalIndexNode(TemporalIndexNode temporalIndexNode) {
        this.temporalIndexNode = temporalIndexNode;
    }

    public void setSpatialIndexNode(SpatialIndexNode spatialIndexNode) {
        this.spatialIndexNode = spatialIndexNode;
    }

    @Override
    void print() {
        System.out.println(toString());
    }

    @Override
    public String toString() {
        return "(" +
                "blockId=" + super.getBlockId() + ", \n" +
                temporalIndexNode +
                spatialIndexNode +
                ")\n ";
    }
}
