package com.rogerguo.test.index.util;

import com.rogerguo.test.index.*;

import java.io.PrintStream;
import java.util.List;

/**
 * @Description  https://www.baeldung.com/java-print-binary-tree-diagram
 * @Date 2021/4/6 17:11
 * @Created by GUO Yang
 */
public class TreePrinter {

    private SpatialTemporalTree tree;

    public TreePrinter(SpatialTemporalTree tree) {
        this.tree = tree;
    }

    public static void main(String[] args) {
        SpatialTemporalTree tree = new SpatialTemporalTree(2);
        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(10, 1, 1);
        for (TrajectorySegmentMeta meta : metaList) {
            tree.insert(meta);
        }
        new TreePrinter(tree).print(System.out);
    }

    public void print(PrintStream os) {
        os.print(traversePreOrder(tree.getRoot()));
    }

    public String traversePreOrder(TreeNode root) {
        if (root == null) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(root.getBlockId());
        String pointer = "├──";


        if (root instanceof InternalNode) {
            InternalNode internalNode = (InternalNode) root;
            stringBuilder.append(" : ").append(internalNode.getTuples());
            List<InternalNodeTuple> internalNodeTuples = internalNode.getTuples();
            for(int i = 0; i < internalNodeTuples.size(); i++) {
                InternalNodeTuple tuple = internalNodeTuples.get(i);
                if (i != internalNodeTuples.size() - 1) {
                    traverseNodes(stringBuilder, "", pointer, tuple.getNodePointer(), true);
                } else {
                    traverseNodes(stringBuilder, "", pointer, tuple.getNodePointer(), false);
                }
            }
        }

        if (root instanceof LeafNode) {
            LeafNode leafNode = (LeafNode) root;

            stringBuilder.append("temporal");
            stringBuilder.append(leafNode.getTemporalIndexNode().getTuples());
            stringBuilder.append("\n");

            stringBuilder.append("spatial");
            stringBuilder.append(leafNode.getSpatialIndexNode().getTuples());
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }

    public void traverseNodes(StringBuilder stringBuilder, String padding, String pointer, TreeNode treeNode, boolean hasRightSibling) {
        if (treeNode != null) {
            stringBuilder.append("\n");
            stringBuilder.append(padding);
            stringBuilder.append(pointer);
            stringBuilder.append(treeNode.getBlockId());

            StringBuilder paddingBuilder = new StringBuilder(padding);
            if (hasRightSibling) {
                paddingBuilder.append("│  ");
            } else {
                paddingBuilder.append("   ");
            }
            String paddingForAll = paddingBuilder.toString();
            String pointerForFirst = "└──";


            if (treeNode instanceof InternalNode) {
                InternalNode internalNode = (InternalNode) treeNode;
                stringBuilder.append(" : ").append(internalNode.getTuples());
                List<InternalNodeTuple> internalNodeTuples = internalNode.getTuples();
                for(int i = 0; i < internalNodeTuples.size(); i++) {
                    InternalNodeTuple tuple = internalNodeTuples.get(i);
                    if (i != internalNodeTuples.size() - 1) {
                        traverseNodes(stringBuilder, paddingForAll, pointerForFirst, tuple.getNodePointer(), true);
                    } else {
                        traverseNodes(stringBuilder, paddingForAll, pointerForFirst, tuple.getNodePointer(), false);
                    }
                }
            }

            if (treeNode instanceof LeafNode) {
                LeafNode leafNode = (LeafNode) treeNode;
                stringBuilder.append("\n");
                stringBuilder.append(paddingForAll);
                stringBuilder.append("temporal");
                stringBuilder.append(leafNode.getTemporalIndexNode().getTuples());

                stringBuilder.append("\n");
                stringBuilder.append(paddingForAll);
                stringBuilder.append("spatial");
                stringBuilder.append(leafNode.getSpatialIndexNode().getTuples());
            }
        }
    }

    public void printOld(PrintStream os) {
        StringBuilder stringBuilder = new StringBuilder();
        traversePreOrderOld(stringBuilder, "","", this.tree.getRoot());
        os.print(stringBuilder.toString());
    }

    public void traversePreOrderOld(StringBuilder stringBuilder, String padding, String pointer, TreeNode treeNode) {
        if (treeNode != null) {
            stringBuilder.append(padding);
            stringBuilder.append(pointer);
            stringBuilder.append(treeNode.getBlockId());
            stringBuilder.append("\n");

            StringBuilder paddingBuilder = new StringBuilder(padding);
            paddingBuilder.append("│  ");
            String paddingForAll = paddingBuilder.toString();
            String pointerForFirst = "└──";
            String pointerForRemaining = "├──";


            if (treeNode instanceof InternalNode) {
                InternalNode internalNode = (InternalNode) treeNode;
                for(InternalNodeTuple tuple : internalNode.getTuples()) {
                    traversePreOrderOld(stringBuilder, paddingForAll, pointerForFirst, tuple.getNodePointer());
                }
            }

            if (treeNode instanceof LeafNode) {
                LeafNode leafNode = (LeafNode) treeNode;
                stringBuilder.append(paddingForAll);
                stringBuilder.append("temporal");
                stringBuilder.append(leafNode.getTemporalIndexNode().getTuples());
                stringBuilder.append("\n");
                stringBuilder.append(paddingForAll);
                stringBuilder.append("spatial");
                stringBuilder.append(leafNode.getSpatialIndexNode().getTuples());
                stringBuilder.append("\n");
            }
        }
    }

}
