package com.rogerguo.test.store;

import com.rogerguo.test.common.DimensionNormalizer;
import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.compression.NormalizedDimensionHelper;
import com.rogerguo.test.util.ZCurve;

import java.util.*;

/**
 *
 * @author yangguo
 * @create 2021-11-22 4:01 PM
 **/
public class HeadChunkIndexWithGeoHashPhysicalSplit implements HeadChunkIndex {

    private int geoHashShiftLength = 16;    // should be an even number

    private DimensionNormalizer dimensionNormalizer = new DimensionNormalizer(-180d, 180d, -90d, 90d);;

    private Map<InvertedIndexKey, Set<String>> invertedIndexForHeadChunk = new HashMap<>();  // key is grid id, value is a posting list which contains the trajectory ids in this grid

    private Map<String, Set<InvertedIndexKey>> idGridMap = new HashMap<>();  // key is trajectory id, value is a set of grids where the trajectory is located at. used for remove

    private Map<Long, DepthTableValue> depthTable = new HashMap<>();  // key is common grid id, value is its depth and the count of trajectory in the common grid

    private int postingListCapacity = 100;

    private SeriesStore seriesStore;

    private ZCurve zCurve = new ZCurve();

    public HeadChunkIndexWithGeoHashPhysicalSplit(SeriesStore seriesStore) {
        this.seriesStore = seriesStore;
    }

    public HeadChunkIndexWithGeoHashPhysicalSplit(int geoHashShiftLength, DimensionNormalizer dimensionNormalizer, int postingListCapacity, SeriesStore seriesStore) {
        this.geoHashShiftLength = geoHashShiftLength;
        this.dimensionNormalizer = dimensionNormalizer;
        this.postingListCapacity = postingListCapacity;
        this.seriesStore = seriesStore;
    }

    public HeadChunkIndexWithGeoHashPhysicalSplit(int geoHashShiftLength, int postingListCapacity, SeriesStore seriesStore) {
        this.geoHashShiftLength = geoHashShiftLength;
        this.postingListCapacity = postingListCapacity;
        this.seriesStore = seriesStore;
    }

    public HeadChunkIndexWithGeoHashPhysicalSplit() { }

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

        String status = "[GeoHash Physical Split] CONFIG: maxDepthLimt = " + (geoHashShiftLength / 2) + "; geoHashShiftLength = " + geoHashShiftLength + "; postingListCapacity = " + postingListCapacity + "\n";
        status = status + "STATUS: number of grid key: " + numberOfKey + "; current max depth: " + maxDepth + "; average posting list size: " + totalPostingLiseSize/numberOfKey + "\n";
        return status;
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
                    // physical split
                    int postingListSizeBefore = postingList.size();
                    postingList.add(point.getOid());
                    int postingListSizeAfter = physicalSplit(invertedIndexForHeadChunk, fineGrainedIndexKey, seriesStore, depthTableValue.getDepth());

                    // update id grid map and depth table
                    depthTableValue.setDepth((byte)(depthTableValue.getDepth() + 1));
                    depthTableValue.setCount(depthTableValue.getCount() + (postingListSizeAfter - postingListSizeBefore));

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
                    // do physical split
                    int postingListSizeBefore = postingList.size();
                    postingList.add(point.getOid());
                    int postingListSizeAfter = physicalSplit(invertedIndexForHeadChunk, geoHashIndexKey, seriesStore, (byte)0);

                    // update id grid map and depth table
                    DepthTableValue depthTableValue = new DepthTableValue((byte)1, postingListSizeAfter);
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

    /**
     * not consider recursive split now
     * @param invertedIndex
     * @param splitIndexKey
     * @param seriesStore
     * @param oldDepth
     */
    private int physicalSplit(Map<InvertedIndexKey, Set<String>> invertedIndex, InvertedIndexKey splitIndexKey, SeriesStore seriesStore, byte oldDepth) {
        Set<String> seriesIdSet = invertedIndex.get(splitIndexKey);
        Set<InvertedIndexKey> subGridKeys = new HashSet<>();
        for (String seriesId: seriesIdSet) {
            Chunk chunk = seriesStore.getSeriesStore().get(seriesId).getHeadChunk();
            for (TrajectoryPoint point : chunk.getChunk()) {
                int x = dimensionNormalizer.normalizeDimensionX(point.getLongitude());
                int y = dimensionNormalizer.normalizeDimensionY(point.getLatitude());
                long rawGeoHash = zCurve.getCurveValue(x, y);

                int shiftLength = Math.max(geoHashShiftLength - (oldDepth + 1) * 2, 0);
                long geoHash = (rawGeoHash >> shiftLength ) << shiftLength;
                InvertedIndexKey geoHashIndexKey = InvertedIndexKey.generateInvertedIndex(geoHash, (byte)(oldDepth + 1));
                subGridKeys.add(geoHashIndexKey);
                if (invertedIndex.containsKey(geoHashIndexKey)) {
                    invertedIndex.get(geoHashIndexKey).add(point.getOid());
                    updateIdGridMap(idGridMap, point, geoHashIndexKey);
                    idGridMap.get(point.getOid()).remove(splitIndexKey);
                } else {
                    Set<String> idSet = new HashSet<>();
                    idSet.add(point.getOid());
                    invertedIndex.put(geoHashIndexKey, idSet);
                    updateIdGridMap(idGridMap, point, geoHashIndexKey);
                    idGridMap.get(point.getOid()).remove(splitIndexKey);
                }
            }
        }
        invertedIndex.remove(splitIndexKey);

        int count = 0;
        for (InvertedIndexKey key : subGridKeys) {
            count = count + invertedIndex.get(key).size();
        }
        return count;
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

    /*private List<InvertedIndexKey> generateFourSubGridIds(long gridId, byte depth) {

        List<InvertedIndexKey> result = new ArrayList<>();

        InvertedIndexKey invertedIndexKey = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 0) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey);
        InvertedIndexKey invertedIndexKey1 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 1) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey1);
        InvertedIndexKey invertedIndexKey2 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 2) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey2);
        InvertedIndexKey invertedIndexKey3 = InvertedIndexKey.generateInvertedIndex(((gridId >> (geoHashShiftLength - 2 * depth)) + 3) << (geoHashShiftLength - 2 * depth), depth);
        result.add(invertedIndexKey3);
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

    public Map<InvertedIndexKey, Set<String>> getInvertedIndexForHeadChunk() {
        return invertedIndexForHeadChunk;
    }

    public Map<Long, DepthTableValue> getDepthTable() {
        return depthTable;
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
