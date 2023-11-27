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
 * @create 2021-11-16 3:48 PM
 **/
public class HeadChunkIndexWithGeoHash implements HeadChunkIndex {

    private int geoHashShiftLength = 16;  // should be an even number, each grid contain 2^16 basic unit, 1 longitude unit is 0.00017, 1 latitude unit is 0.000085

    private DimensionNormalizer dimensionNormalizer = new DimensionNormalizer(-180d, 180d, -90d, 90d);;

    private Map<Long, Set<String>> invertedIndexForHeadChunk = new HashMap<>();

    private Map<String, Set<Long>> idGridMap = new HashMap<>();  // used for remove

    private ZCurve zCurve = new ZCurve();

    public HeadChunkIndexWithGeoHash() {}

    public HeadChunkIndexWithGeoHash(int geoHashShiftLength, DimensionNormalizer dimensionNormalizer) {
        this.geoHashShiftLength = geoHashShiftLength;
        this.dimensionNormalizer = dimensionNormalizer;
    }

    public HeadChunkIndexWithGeoHash(int geoHashShiftLength) {
        this.geoHashShiftLength = geoHashShiftLength;
    }

    @Override
    public String printStatus() {
        int numberOfKey = invertedIndexForHeadChunk.size();

        int totalPostingLiseSize = 0;
        for (Set<String> list : invertedIndexForHeadChunk.values()) {
            totalPostingLiseSize = totalPostingLiseSize + list.size();
        }

        String status = "[GeoHash] CONFIG: geoHashShiftLength = " + (geoHashShiftLength) +   "\n";
        status = status + "STATUS: number of grid key: " + numberOfKey + "; average posting list size: " + totalPostingLiseSize/numberOfKey + "\n";
        return status;
    }

    public void updateIndex(TrajectoryPoint point) {
        int x = dimensionNormalizer.normalizeDimensionX(point.getLongitude());
        int y = dimensionNormalizer.normalizeDimensionY(point.getLatitude());
        long geoHash = zCurve.getCurveValue(x, y) >> geoHashShiftLength;

        if (invertedIndexForHeadChunk.containsKey(geoHash)) {
            invertedIndexForHeadChunk.get(geoHash).add(point.getOid());
        } else {
            // we have a new spatial grid
            Set<String> postingList = new HashSet<>();
            postingList.add(point.getOid());
            invertedIndexForHeadChunk.put(geoHash, postingList);
        }

        if (idGridMap.containsKey(point.getOid())) {
            idGridMap.get(point.getOid()).add(geoHash);
        } else {
            Set<Long> set = new HashSet<>();
            set.add(geoHash);
            idGridMap.put(point.getOid(), set);
        }
    }

    public void removeFromIndex(Chunk headChunk) {
        Set<Long> gridList = idGridMap.get(headChunk.getSid());
        for (long gridId : gridList) {
            if (invertedIndexForHeadChunk.containsKey(gridId)) {
                invertedIndexForHeadChunk.get(gridId).remove(headChunk.getSid());
                if (invertedIndexForHeadChunk.get(gridId).size() == 0) {
                    invertedIndexForHeadChunk.remove(gridId);
                }
            }
        }
        idGridMap.remove(headChunk.getSid());
    }

    public Set<String> searchForSpatial(SpatialBoundingBox spatialBoundingBox) {
        Point lowerLeftPoint = spatialBoundingBox.getLowerLeft();
        Point upperRightPoint = spatialBoundingBox.getUpperRight();

        int normalizedLonMin = dimensionNormalizer.normalizeDimensionX(lowerLeftPoint.getLongitude());
        int normalizedLonMax = dimensionNormalizer.normalizeDimensionX(upperRightPoint.getLongitude());
        int normalizedLatMin = dimensionNormalizer.normalizeDimensionY(lowerLeftPoint.getLatitude());
        int normalizedLatMax = dimensionNormalizer.normalizeDimensionY(upperRightPoint.getLatitude());

        //System.out.println(zCurve.from2DCurveValue(zCurve.getCurveValue(normalizedLonMin, normalizedLatMin) >> geoHashShiftLength));
        //System.out.println(zCurve.from2DCurveValue(zCurve.getCurveValue(normalizedLonMax, normalizedLatMax) >> geoHashShiftLength));

        Set<String> result = new HashSet<>();
        int shift = geoHashShiftLength / 2;
        for (int xIndex = (normalizedLonMin >> shift); xIndex <= (normalizedLonMax >> shift); xIndex++ ) {
            for (int yIndex = (normalizedLatMin >> shift); yIndex <= (normalizedLatMax >> shift); yIndex++) {
                //System.out.println("x: " + xIndex + "; y: " + yIndex);
                long geoHashPrefix = zCurve.getCurveValue(xIndex, yIndex);
                Set<String> values = invertedIndexForHeadChunk.get(geoHashPrefix);
                if (values != null) {
                    result.addAll(values);
                }
            }
        }

        return result;
    }

    public Map<Long, Set<String>> getInvertedIndexForHeadChunk() {
        return invertedIndexForHeadChunk;
    }

    public int getGeoHashShiftLength() {
        return geoHashShiftLength;
    }

    @Override
    public String toString() {
        return "HeadChunkIndexWithGeoHash{" +
                "invertedIndexForHeadChunk=" + invertedIndexForHeadChunk +
                ", idGridMap=" + idGridMap +
                '}';
    }
}
