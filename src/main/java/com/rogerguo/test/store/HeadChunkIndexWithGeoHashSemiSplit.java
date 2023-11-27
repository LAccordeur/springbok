package com.rogerguo.test.store;

import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.compression.NormalizedDimensionHelper;
import com.rogerguo.test.util.ZCurve;

import java.util.*;

/**
 * @author yangguo
 * @create 2021-11-22 10:37 AM
 **/
public class HeadChunkIndexWithGeoHashSemiSplit implements HeadChunkIndex{

    private int geoHashShiftLength = 16;    // should be an even number

    private DimensionNormalizer dimensionNormalizer = new DimensionNormalizer(-180d, 180d, -90d, 90d);

    private Map<InvertedIndexKey, Set<String>> invertedIndexForHeadChunk = new HashMap<>();  // key is grid id, value is a posting list which contains the trajectory ids in this grid

    private Map<String, Set<InvertedIndexKey>> idGridMap = new HashMap<>();  // key is trajectory id, value is a set of grids where the trajectory is located at. used for remove

    private Map<Long, DepthTableValue> depthTable = new HashMap<>();  // key is common grid id, value is its depth and the count of trajectory in the common grid

    private int postingListCapacity = 100;

    private ZCurve zCurve = new ZCurve();

    @Override
    public String printStatus() {
        int numberOfKey = invertedIndexForHeadChunk.size();
        int maxDepth = 0;
        for (DepthTableValue value : depthTable.values()) {
            if (value.getDepth() > maxDepth) {
                maxDepth = value.getDepth();
            }
        }

        int totalPostingLiseSize = 0;
        for (Set<String> list : invertedIndexForHeadChunk.values()) {
            totalPostingLiseSize = totalPostingLiseSize + list.size();
        }

        String status = "[GeoHash Semi Split] CONFIG: maxDepthLimt = " + (geoHashShiftLength / 2) + "; geoHashShiftLength = " + geoHashShiftLength +  "; postingListCapacity = " + postingListCapacity + "\n";
        status = status + "STATUS: number of grid key: " + numberOfKey + "; current max depth: " + maxDepth + "; average posting list size: " + totalPostingLiseSize/numberOfKey + "\n";
        return status;
    }

    public HeadChunkIndexWithGeoHashSemiSplit(int geoHashShiftLength, int postingListCapacity) {
        this.geoHashShiftLength = geoHashShiftLength;
        this.postingListCapacity = postingListCapacity;
    }

    public HeadChunkIndexWithGeoHashSemiSplit(int geoHashShiftLength, DimensionNormalizer dimensionNormalizer, int postingListCapacity) {
        this.geoHashShiftLength = geoHashShiftLength;
        this.dimensionNormalizer = dimensionNormalizer;
        this.postingListCapacity = postingListCapacity;
    }

    public HeadChunkIndexWithGeoHashSemiSplit() {}

    public Map<InvertedIndexKey, Set<String>> getInvertedIndexForHeadChunk() {
        return invertedIndexForHeadChunk;
    }

    public Map<Long, DepthTableValue> getDepthTable() {
        return depthTable;
    }

    @Override
    public void updateIndex(TrajectoryPoint point) {
        int x = dimensionNormalizer.normalizeDimensionX(point.getLongitude());
        int y = dimensionNormalizer.normalizeDimensionY(point.getLatitude());
        long rawGeoHash = zCurve.getCurveValue(x, y);

        long geoHash = (rawGeoHash >> geoHashShiftLength) << geoHashShiftLength;
        InvertedIndexKey geoHashIndexKey = InvertedIndexKey.generateInvertedIndex(geoHash, (byte)0);
        if (depthTable.containsKey(geoHash)) {
            // a fine-grained sub-grid
            DepthTableValue depthTableValue = depthTable.get(geoHash);
            int shiftLength = Math.max((geoHashShiftLength - depthTableValue.getDepth() * 2), 0);
            long fineGrainedGeoHash = (rawGeoHash >> (shiftLength)) << shiftLength;
            InvertedIndexKey fineGrainedIndexKey = InvertedIndexKey.generateInvertedIndex(fineGrainedGeoHash, depthTableValue.getDepth());
            if (invertedIndexForHeadChunk.containsKey(fineGrainedIndexKey)) {
                Set<String> postingList = invertedIndexForHeadChunk.get(fineGrainedIndexKey);
                if (postingList.size() < postingListCapacity) {
                    int beforePostingListSize = postingList.size();
                    postingList.add(point.getOid());
                    // update id grid map and depth table
                    updateIdGridMap(idGridMap, point, fineGrainedIndexKey);
                    int afterPostingListSize = postingList.size();
                    depthTableValue.setCount(depthTableValue.getCount() + (afterPostingListSize - beforePostingListSize));
                } else {
                    // virtual split
                    int shiftLengthForSplit =  Math.max((geoHashShiftLength - (depthTableValue.getDepth() + 1) * 2), 0);
                    long geoHashForSplit = (rawGeoHash >> shiftLengthForSplit) << shiftLengthForSplit;
                    InvertedIndexKey geoHashForSplitIndexKey = InvertedIndexKey.generateInvertedIndex(geoHashForSplit, (byte)(depthTableValue.getDepth() + 1));
                    Set<String> postingListForSplit = new HashSet<>();
                    postingListForSplit.add(point.getOid());
                    invertedIndexForHeadChunk.put(geoHashForSplitIndexKey, postingListForSplit);

                    // update id grid map and depth table
                    updateIdGridMap(idGridMap, point, geoHashForSplitIndexKey);
                    depthTableValue.setDepth((byte)(depthTableValue.getDepth() + 1));
                    depthTableValue.setCount(depthTableValue.getCount() + 1);

                }
            } else {
                // this is a new sub-grid
                Set<String> postingList = new HashSet<>();
                postingList.add(point.getOid());
                invertedIndexForHeadChunk.put(fineGrainedIndexKey, postingList);

                // update id grid map and depth table
                updateIdGridMap(idGridMap, point, fineGrainedIndexKey);
                depthTableValue.setCount(depthTableValue.getCount() + 1);
            }

        } else {
            // a common grid
            if (invertedIndexForHeadChunk.containsKey(geoHashIndexKey)) {
                Set<String> postingList = invertedIndexForHeadChunk.get(geoHashIndexKey);
                if (postingList.size() < postingListCapacity) {
                    postingList.add(point.getOid());
                    updateIdGridMap(idGridMap, point, geoHashIndexKey);

                } else {
                    // do virtual split
                    int shiftLengthForSplit = Math.max((geoHashShiftLength - 1 * 2), 0);
                    long geoHashForSplit = (rawGeoHash >> shiftLengthForSplit) << shiftLengthForSplit;
                    InvertedIndexKey geoHashForSplitIndexKey = InvertedIndexKey.generateInvertedIndex(geoHashForSplit, (byte)1);
                    Set<String> postingListForSplit = new HashSet<>();
                    postingListForSplit.add(point.getOid());
                    invertedIndexForHeadChunk.put(geoHashForSplitIndexKey, postingListForSplit);

                    // update id grid map and depth table
                    updateIdGridMap(idGridMap, point, geoHashForSplitIndexKey);
                    DepthTableValue depthTableValue = new DepthTableValue((byte)1, postingListCapacity + 1);
                    depthTable.put(geoHash, depthTableValue);
                }
            } else {
                // we have a new spatial grid
                Set<String> postingList = new HashSet<>();
                postingList.add(point.getOid());
                invertedIndexForHeadChunk.put(geoHashIndexKey, postingList);
                updateIdGridMap(idGridMap, point, geoHashIndexKey);
            }
        }

    }

    private void updateIdGridMap(Map<String, Set<InvertedIndexKey>> idGridMap, TrajectoryPoint point, InvertedIndexKey geoHash) {
        if (idGridMap.containsKey(point.getOid())) {
            idGridMap.get(point.getOid()).add(geoHash);
        } else {
            Set<InvertedIndexKey> set = new HashSet<>();
            set.add(geoHash);
            idGridMap.put(point.getOid(), set);
        }
    }

    @Override
    public void removeFromIndex(Chunk headChunk) {
        Set<InvertedIndexKey> gridList = idGridMap.get(headChunk.getSid());

        if (gridList == null || gridList.size() == 0) {
            return;
        }

        Map<Long, Integer> reducedCountForCommonGrids = new HashMap<>();
        for (InvertedIndexKey gridId : gridList) {
            if (invertedIndexForHeadChunk.containsKey(gridId)) {
                invertedIndexForHeadChunk.get(gridId).remove(headChunk.getSid());
                if (invertedIndexForHeadChunk.get(gridId).size() == 0) {
                    invertedIndexForHeadChunk.remove(gridId);
                }
            }

            long commonGridId = (gridId.getGridId() >> (geoHashShiftLength)) << geoHashShiftLength;

            if (depthTable.containsKey(commonGridId)) {
                if (reducedCountForCommonGrids.containsKey(commonGridId)) {
                    reducedCountForCommonGrids.put(commonGridId, reducedCountForCommonGrids.get(commonGridId)+1);
                } else {
                    reducedCountForCommonGrids.put(commonGridId, 1);
                }
            }
        }
        idGridMap.remove(headChunk.getSid());

        // update depth table and check merge
        for (long gridId : reducedCountForCommonGrids.keySet()) {
            int reducedCount = reducedCountForCommonGrids.get(gridId);
            // update depth table
            depthTable.get(gridId).setCount(depthTable.get(gridId).getCount() - reducedCount);
            if (depthTable.get(gridId).getCount() < postingListCapacity) {
                // do merge
                byte depth = depthTable.get(gridId).getDepth();
                Set<String> mergedPostingList = new HashSet<>();
                List<InvertedIndexKey> subKeyList = generateSubGridIds(gridId, depth);
                List<InvertedIndexKey> satisfiedSubKeyList = new ArrayList<>();
                for (InvertedIndexKey subGridId : subKeyList) {
                    if (invertedIndexForHeadChunk.containsKey(subGridId)) {
                        mergedPostingList.addAll(invertedIndexForHeadChunk.get(subGridId));
                        satisfiedSubKeyList.add(subGridId);
                    }
                }
                depthTable.remove(gridId);
                InvertedIndexKey gridIdIndexKey = InvertedIndexKey.generateInvertedIndex(gridId, (byte) 0);
                invertedIndexForHeadChunk.put(gridIdIndexKey, mergedPostingList);
                for (InvertedIndexKey subGridId : satisfiedSubKeyList) {
                    invertedIndexForHeadChunk.remove(subGridId);
                }
            }
        }

    }

    /**
     * generate all sub grid ids under this commonGridId util reaching depth
     * for example: assume geoShiftLength is 8, we have common grid id 0001 0000 0000 and depth is 2,
     * then 0001 0000 0000, 0001 0100 0000, 0001 1000 0000, 0001 1100 0000
     *      0001 0000 0000, 0001 0001 0000, 0001 0010 0000, 0001 0011 0000
     *      0001 0100 0000, 0001 0101 0000, 0001 0110 0000, 0001 0111 0000
     *      0001 1000 0000, 0001 1001 0000, 0001 1010 0000, 0001 1011 0000
     *      0001 1100 0000, 0001 1101 0000, 0001 1110 0000, 0001 1111 0000
     * @param commonGridId is the top-level grid id
     * @param maxDepth
     * @return
     */
    public List<InvertedIndexKey> generateSubGridIds(long commonGridId, byte maxDepth) {
        List<InvertedIndexKey> idList = new ArrayList<>();

        Queue<InvertedIndexKey> queue = new LinkedList<>();
        queue.add(InvertedIndexKey.generateInvertedIndex(commonGridId, (byte)0));

        while (!queue.isEmpty()) {
            InvertedIndexKey currentKey = queue.poll();
            if (currentKey.getDepth() >= maxDepth) {
                break;
            }
            List<InvertedIndexKey> subGridIdList = generateForSubGridKeys(currentKey);
            idList.addAll(subGridIdList);
            queue.addAll(subGridIdList);
        }

        return idList;
    }

    /**
     * generate all sub grid ids under this commonGridId util reaching depth
     * and filter out non-overlapped ones
     * @param commonGridId is the top-level grid id
     * @param maxDepth
     * @return
     */
    public List<InvertedIndexKey> generateSubGridIds(long commonGridId, byte maxDepth, int normalizedLonMin, int normalizedLatMin, int normalizedLonMax, int normalizedLatMax) {
        List<InvertedIndexKey> idList = new ArrayList<>();

        Queue<InvertedIndexKey> queue = new LinkedList<>();
        queue.add(InvertedIndexKey.generateInvertedIndex(commonGridId, (byte)0));

        while (!queue.isEmpty()) {
            InvertedIndexKey currentKey = queue.poll();
            if (currentKey.getDepth() >= maxDepth) {
                break;
            }
            List<InvertedIndexKey> subGridIdList = generateForSubGridKeys(currentKey);
            for (InvertedIndexKey key : subGridIdList) {
                long subGridId = key.getGridId();
                byte depth = key.getDepth();
                int validShiftLengthForSubGrid = Math.max((geoHashShiftLength - depth * 2), 0);
                int subGridEdgeLength = (int) Math.pow(2, validShiftLengthForSubGrid/2);

                Map<String, Integer> gridMap = zCurve.from2DCurveValue(subGridId);
                int gridXMin = gridMap.get("x");
                int gridXMax = gridXMin + subGridEdgeLength - 1;
                int gridYMin = gridMap.get("y");
                int gridYMax = gridYMin + subGridEdgeLength - 1;
                if (gridXMax >= normalizedLonMin && gridXMin <= normalizedLonMax && gridYMax >= normalizedLatMin && gridYMin <= normalizedLatMax) {
                    idList.add(key);
                    queue.add(key);
                }
            }
        }

        return idList;
    }

    /**
     * generate the next depth subgrid for this @key
     * @param key
     * @return
     */
    public List<InvertedIndexKey> generateForSubGridKeys(InvertedIndexKey key) {
        List<InvertedIndexKey> result = new ArrayList<>();
        long rawGridId = key.getGridId();
        byte rawGridDepth = key.getDepth();
        byte subGridDepth = (byte) (rawGridDepth + 1);

        int shiftLengthForSubGrids = geoHashShiftLength - 2 * subGridDepth;
        if (shiftLengthForSubGrids >= 0) {
            InvertedIndexKey invertedIndexKey = InvertedIndexKey.generateInvertedIndex(((rawGridId >> shiftLengthForSubGrids) + 0) << shiftLengthForSubGrids, subGridDepth);
            result.add(invertedIndexKey);
            InvertedIndexKey invertedIndexKey1 = InvertedIndexKey.generateInvertedIndex(((rawGridId >> shiftLengthForSubGrids) + 1) << shiftLengthForSubGrids, subGridDepth);
            result.add(invertedIndexKey1);
            InvertedIndexKey invertedIndexKey2 = InvertedIndexKey.generateInvertedIndex(((rawGridId >> shiftLengthForSubGrids) + 2) << shiftLengthForSubGrids, subGridDepth);
            result.add(invertedIndexKey2);
            InvertedIndexKey invertedIndexKey3 = InvertedIndexKey.generateInvertedIndex(((rawGridId >> shiftLengthForSubGrids) + 3) << shiftLengthForSubGrids, subGridDepth);
            result.add(invertedIndexKey3);
        } else {
            result.add(InvertedIndexKey.generateInvertedIndex(key.getGridId(), subGridDepth));
        }
        return result;
    }


    /*@Deprecated
    public List<InvertedIndexKey> generateFourSubGridIds(long gridId, byte depth) {

        List<InvertedIndexKey> result = new ArrayList<>();

        *//*InvertedIndexKey invertedIndexKey = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 0) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey);
        InvertedIndexKey invertedIndexKey1 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 1) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey1);
        InvertedIndexKey invertedIndexKey2 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 2) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey2);
        InvertedIndexKey invertedIndexKey3 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 3) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey3);*//*
        if ((geoHashShiftLength - 2* depth) >= 0) {
            InvertedIndexKey invertedIndexKey = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 0) << (geoHashShiftLength - 2 * depth), depth);
            result.add(invertedIndexKey);
            InvertedIndexKey invertedIndexKey1 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 1) << (geoHashShiftLength - 2 * depth), depth);
            result.add(invertedIndexKey1);
            InvertedIndexKey invertedIndexKey2 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 2) << (geoHashShiftLength - 2 * depth), depth);
            result.add(invertedIndexKey2);
            InvertedIndexKey invertedIndexKey3 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 3) << (geoHashShiftLength - 2 * depth), depth);
            result.add(invertedIndexKey3);
        } else {
            result.add(InvertedIndexKey.generateInvertedIndex(gridId, depth));
        }


        return result;
    }*/

    @Override
    public Set<String> searchForSpatial(SpatialBoundingBox spatialBoundingBox) {
        Point lowerLeftPoint = spatialBoundingBox.getLowerLeft();
        Point upperRightPoint = spatialBoundingBox.getUpperRight();

        int normalizedLonMin = dimensionNormalizer.normalizeDimensionX(lowerLeftPoint.getLongitude());
        int normalizedLonMax = dimensionNormalizer.normalizeDimensionX(upperRightPoint.getLongitude());
        int normalizedLatMin = dimensionNormalizer.normalizeDimensionY(lowerLeftPoint.getLatitude());
        int normalizedLatMax = dimensionNormalizer.normalizeDimensionY(upperRightPoint.getLatitude());



        Set<String> result = new HashSet<>();
        int shift = geoHashShiftLength / 2;
        for (int xIndex = (normalizedLonMin >> shift); xIndex <= (normalizedLonMax >> shift); xIndex++ ) {
            for (int yIndex = (normalizedLatMin >> shift); yIndex <= (normalizedLatMax >> shift); yIndex++) {
                //System.out.println("x: " + xIndex + "; y: " + yIndex);
                long geoHashPrefix = zCurve.getCurveValue(xIndex, yIndex);
                geoHashPrefix = geoHashPrefix << geoHashShiftLength;
                InvertedIndexKey geoHashIndexKey = InvertedIndexKey.generateInvertedIndex(geoHashPrefix, (byte)0);
                if (depthTable.containsKey(geoHashPrefix)) {
                    List<InvertedIndexKey> subGridIdList = generateSubGridIds(geoHashPrefix, depthTable.get(geoHashPrefix).getDepth(), normalizedLonMin, normalizedLatMin, normalizedLonMax, normalizedLatMax);
                    for (InvertedIndexKey subGridId : subGridIdList) {
                        if (invertedIndexForHeadChunk.get(subGridId) != null) {
                            result.addAll(invertedIndexForHeadChunk.get(subGridId));
                        }
                    }
                }
                if (invertedIndexForHeadChunk.get(geoHashIndexKey) != null) {
                    result.addAll(invertedIndexForHeadChunk.get(geoHashIndexKey));
                }
            }
        }

        return result;
    }

    @Override
    public String toString() {
        return "HeadChunkIndexWithGeoHashVirtualSplit{" +
                "invertedIndexForHeadChunk=" + invertedIndexForHeadChunk +
                ", idGridMap=" + idGridMap +
                ", depthTable=" + depthTable +
                '}';
    }
}

class InvertedIndexKey {
    private long gridId;

    private byte depth;  // depth for common grids is 0

    //private static Map<Long, Map<Byte, InvertedIndexKey>> map = new HashMap<>();

    public InvertedIndexKey(long gridId, byte depth) {
        this.gridId = gridId;
        this.depth = depth;
    }

    public static InvertedIndexKey generateInvertedIndex(long gridId, byte depth) {
        /*if (map.containsKey(gridId)) {
            Map<Byte, InvertedIndexKey> map2 = map.get(gridId);
            if (map2.containsKey(depth)) {
                return map2.get(depth);
            } else {
                InvertedIndexKey invertedIndexKey = new InvertedIndexKey(gridId, depth);
                map2.put(depth, invertedIndexKey);
                return invertedIndexKey;
            }
        } else {
            InvertedIndexKey invertedIndexKey = new InvertedIndexKey(gridId, depth);
            Map<Byte, InvertedIndexKey> map2 = new HashMap<>();
            map2.put(depth, invertedIndexKey);
            map.put(gridId, map2);
            return invertedIndexKey;
        }*/

        return new InvertedIndexKey(gridId, depth);
    }

    public long getGridId() {
        return gridId;
    }

    public void setGridId(long gridId) {
        this.gridId = gridId;
    }

    public byte getDepth() {
        return depth;
    }

    public void setDepth(byte depth) {
        this.depth = depth;
    }

    @Override
    public int hashCode() {
        return (int) (gridId * 31 + depth);  // TODO check efficiency
    }

    @Override
    public boolean equals(Object obj) {
        InvertedIndexKey keyObj = (InvertedIndexKey) obj;

        if (this.gridId == keyObj.gridId && this.depth == keyObj.depth) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "InvertedIndexKey{" +
                "gridId=" + gridId +
                ", depth=" + depth +
                '}';
    }
}

class DepthTableValue {

    byte depth;

    int count;

    public DepthTableValue(byte depth, int count) {
        this.depth = depth;
        this.count = count;
    }

    public byte getDepth() {
        return depth;
    }

    public void setDepth(byte depth) {
        this.depth = depth;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "DepthTableValue{" +
                "depth=" + depth +
                ", count=" + count +
                '}';
    }
}
