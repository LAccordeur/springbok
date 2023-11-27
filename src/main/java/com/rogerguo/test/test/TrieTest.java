package com.rogerguo.test.test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author yangguo
 * @create 2021-11-22 7:03 PM
 **/
@Deprecated
public class TrieTest {


    public static void generateSyntheticData() {
        String code1 = "00000000";
    }

}

@Deprecated
class SpatialTrie {
    private SpatialTrie[] children;
    private SpatialTrie parent = null;
    private int index;
    private boolean isEnd;

    public SpatialTrie() {
        children = new SpatialTrie[4];
        isEnd = false;
    }

    public SpatialTrie(SpatialTrie parent, int index) {
        this.children = new SpatialTrie[4];
        this.parent = parent;
        this.isEnd = false;
        this.index = index;
    }

    public void insert(String spatialCode) {
        SpatialTrie node = this;
        for (int i = 0; i < spatialCode.length(); i = i + 2) {
            String ch = spatialCode.substring(i, i+2);
            int index = 0;
            if (ch.equals("00")) {
                index = 0;
            } else if (ch.equals("01")) {
                index = 1;
            } else if (ch.equals("10")) {
                index = 2;
            } else if (ch.equals("11")) {
                index = 3;
            }
            if (node.children[index] == null) {
                node.children[index] = new SpatialTrie(node, index);
            }
            node = node.children[index];
        }
        node.isEnd = true;
    }

    public List<String> calculateSpatialRepresentation(int steps) {
        List<SpatialTrie> resultNodeList = new ArrayList<>();
        List<String> resultList = new ArrayList<>();

        Queue<SpatialTrie> queue = new LinkedList<>();
        SpatialTrie node = this;
        queue.add(node);
        int currentStep = 0;
        while (!queue.isEmpty() && currentStep < steps) {
            SpatialTrie currentNode = queue.poll();
            currentStep++;
            if (currentNode.children[0] != null && currentNode.children[1] != null
            && currentNode.children[2] != null && currentNode.children[3] != null) {

                resultNodeList.add(currentNode);

            } else {
                if (currentNode.children[0] != null) {
                    if (currentStep == steps - 1) {
                        resultNodeList.add(currentNode.children[0]);
                    } else {
                        queue.add(currentNode.children[0]);
                    }
                }
                if (currentNode.children[1] != null) {
                    if (currentStep == steps - 1) {
                        resultNodeList.add(currentNode.children[1]);
                    } else {
                        queue.add(currentNode.children[1]);
                    }
                }
                if (currentNode.children[2] != null) {
                    if (currentStep == steps - 1) {
                        resultNodeList.add(currentNode.children[2]);
                    } else {
                        queue.add(currentNode.children[2]);
                    }
                }

                if (currentNode.children[3] != null) {
                    if (currentStep == steps - 1) {
                        resultNodeList.add(currentNode.children[3]);
                    } else {
                        queue.add(currentNode.children[3]);
                    }
                }

            }
        }

        List<String> items = new ArrayList<>();
        for (SpatialTrie resultNode : resultNodeList) {
            SpatialTrie currentNode = resultNode;
            while (currentNode.parent != null) {
                if (index == 0) {
                    items.add("00");
                } else if (index == 1) {
                    items.add("01");
                } else if (index == 2) {
                    items.add("10");
                } else if (index == 3) {
                    items.add("11");
                }
                currentNode = currentNode.parent;
            }
            StringBuffer stringBuffer = new StringBuffer();
            for (int i = items.size() - 1; i >=0; i--) {
                stringBuffer.append(items.get(i));
            }
            resultList.add(stringBuffer.toString());
        }


        return resultList;
    }

    public boolean search(String prefix) {
        SpatialTrie node = searchPrefix(prefix);
        return node != null && node.isEnd;
    }

    public boolean startsWith(String prefix) {
        return searchPrefix(prefix) != null;
    }

    private SpatialTrie searchPrefix(String prefix) {
        SpatialTrie node = this;
        for (int i = 0; i < prefix.length(); i = i + 2) {
            String ch = prefix.substring(i, i+2);
            int index = 0;
            if (ch.equals("00")) {
                index = 0;
            } else if (ch.equals("01")) {
                index = 1;
            } else if (ch.equals("10")) {
                index = 2;
            } else if (ch.equals("11")) {
                index = 3;
            }
            if (node.children[index] == null) {
                return null;
            }
            node = node.children[index];
        }
        return node;
    }

    public void setParent(SpatialTrie parent) {
        this.parent = parent;
    }
}
