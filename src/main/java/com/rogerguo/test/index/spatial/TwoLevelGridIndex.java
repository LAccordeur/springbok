package com.rogerguo.test.index.spatial;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.SpatialRange;
import com.rogerguo.test.util.SpaceFillingCurve;
import com.rogerguo.test.util.ZCurve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** used to generate spatial index tuples  (replaced by signature)
 * @author yangguo
 * @create 2021-07-14 3:52 PM
 **/
@Deprecated
public class TwoLevelGridIndex {

    private double levelOneGridLongitudeWidth = 0.05;

    private double levelOneGridLatitudeWidth = 0.05;

    private double levelTwoGridLongitudeWidth = 0.005;

    private double levelTwoGridLatitudeWidth = 0.005;

    private SpaceFillingCurve spaceFillingCurve = new ZCurve();

    private static Logger logger = LoggerFactory.getLogger(TwoLevelGridIndex.class);

    public TwoLevelGridIndex(double levelOneGridLongitudeWidth, double levelOneGridLatitudeWidth, double levelTwoGridLongitudeWidth, double levelTwoGridLatitudeWidth) {
        this.levelOneGridLongitudeWidth = levelOneGridLongitudeWidth;
        this.levelOneGridLatitudeWidth = levelOneGridLatitudeWidth;
        this.levelTwoGridLongitudeWidth = levelTwoGridLongitudeWidth;
        this.levelTwoGridLatitudeWidth = levelTwoGridLatitudeWidth;
    }

    public TwoLevelGridIndex() {}

    /**
     * given a bounding box, return the identifiers of overlapped grids
     * @param boundingBox
     * @return
     */
    @Deprecated
    public List<Long> toIndexGrids(SpatialBoundingBox boundingBox) {

        List<Long> indexGrids = new ArrayList<>();

        int levelOneLonMin = (int) Math.floor(boundingBox.getLowerLeft().getLongitude() / levelOneGridLongitudeWidth);
        int levelOneLonMax = (int) Math.floor(boundingBox.getUpperRight().getLongitude() / levelOneGridLongitudeWidth);
        int levelOneLatMin = (int) Math.floor(boundingBox.getLowerLeft().getLatitude() / levelOneGridLatitudeWidth);
        int levelOneLatMax = (int) Math.floor(boundingBox.getUpperRight().getLatitude() / levelOneGridLatitudeWidth);

        for (int levelOneLonIndex = levelOneLonMin; levelOneLonIndex <= levelOneLonMax; levelOneLonIndex++) {
            for (int levelOneLatIndex = levelOneLatMin; levelOneLatIndex <= levelOneLatMax; levelOneLatIndex++) {
                // for each level one grid, we find the overlapped level two grids
                SpatialBoundingBox levelOneGridBoundingBox = getBoundingBoxByLevelOneGridCoordinate(levelOneLonIndex, levelOneLatIndex);
                SpatialBoundingBox overlappedBoundingBox = getOverlappedBoundingBox(levelOneGridBoundingBox, boundingBox);
                if (overlappedBoundingBox != null) {
                    int levelTwoLonMin = (int) Math.floor((overlappedBoundingBox.getLowerLeft().getLongitude() - levelOneGridBoundingBox.getLowerLeft().getLongitude()) / levelTwoGridLongitudeWidth);
                    int levelTwoLonMax = (int) Math.floor((overlappedBoundingBox.getUpperRight().getLongitude() - levelOneGridBoundingBox.getLowerLeft().getLongitude()) / levelTwoGridLongitudeWidth);
                    int levelTwoLatMin = (int) Math.floor((overlappedBoundingBox.getLowerLeft().getLatitude() - levelOneGridBoundingBox.getLowerLeft().getLatitude()) / levelTwoGridLatitudeWidth);
                    int levelTwoLatMax = (int) Math.floor((overlappedBoundingBox.getUpperRight().getLatitude() - levelOneGridBoundingBox.getLowerLeft().getLatitude()) / levelTwoGridLatitudeWidth);

                    long levelOneGridId = spaceFillingCurve.getCurveValue(levelOneLonIndex, levelOneLatIndex);
                    for (int levelTwoLonIndex = levelTwoLonMin; levelTwoLonIndex <= levelTwoLonMax; levelTwoLonIndex++) {
                        for (int levelTwoLatIndex = levelTwoLatMin; levelTwoLatIndex <= levelTwoLatMax; levelTwoLatIndex++) {
                            long levelTwoGridId = spaceFillingCurve.getCurveValue(levelTwoLonIndex, levelTwoLatIndex);
                            long combinedId = toCombinedGridIdentifier(levelOneGridId, levelTwoGridId);
                            indexGrids.add(combinedId);
                        }
                    }
                }
            }
        }


        return indexGrids;
    }

    /**
     *
     * @param boundingBox
     * @param pointList  the point list that in this bounding box
     * @return
     */
    @Deprecated
    public List<Long> toPreciseIndexGrids(SpatialBoundingBox boundingBox, List<? extends Point> pointList) {
        List<Long> indexGridList = toIndexGrids(boundingBox);

        // filter out non-overlapped grids
        Iterator<Long> iterator = indexGridList.iterator();
        while (iterator.hasNext()) {
            Long gridId = iterator.next();
            SpatialBoundingBox gridBoundingBox = getBoundingBoxByGridId(gridId);
            boolean mark = false;
            for (Point point : pointList) {
                if (checkBoundingBoxContainPoint(gridBoundingBox, point)) {
                    mark = true;
                    break;
                }
            }
            if (!mark) {
                iterator.remove();
            }
        }

        logger.info("spatial region: {" + boundingBox.getLowerLeft() + ", " + boundingBox.getUpperRight() + "} the number of generated grids: " + indexGridList.size());

        return indexGridList;
    }

    public Set<Long> toPreciseIndexGridsOptimized(List<? extends Point> pointList) {

        Set<Long> resultGridSet = new HashSet<>();

        for (Point point : pointList) {
            int levelOneLon = (int) Math.floor(point.getLongitude() / levelOneGridLongitudeWidth);
            int levelOneLat = (int) Math.floor(point.getLatitude() / levelOneGridLatitudeWidth);
            SpatialBoundingBox levelOneGridBoundingBox = getBoundingBoxByLevelOneGridCoordinate(levelOneLon, levelOneLat);
            int levelTwoLon = (int) Math.floor((point.getLongitude() - levelOneGridBoundingBox.getLowerLeft().getLongitude()) / levelTwoGridLongitudeWidth);
            int levelTwoLat = (int) Math.floor((point.getLatitude() - levelOneGridBoundingBox.getLowerLeft().getLatitude()) / levelTwoGridLatitudeWidth);

            long levelOneGridId = spaceFillingCurve.getCurveValue(levelOneLon, levelOneLat);
            long levelTwoGridId = spaceFillingCurve.getCurveValue(levelTwoLon, levelTwoLat);
            resultGridSet.add(toCombinedGridIdentifier(levelOneGridId, levelTwoGridId));
        }

        return resultGridSet;
    }

    /**
     * 范围的开区间闭区间 [right, left)
     * * @param boundingBox
     * @param point
     * @return
     */
    public boolean checkBoundingBoxContainPoint(SpatialBoundingBox boundingBox, Point point) {

        if (point.getLongitude() >= boundingBox.getLowerLeft().getLongitude() && point.getLongitude() < boundingBox.getUpperRight().getLongitude()
        && point.getLatitude() >= boundingBox.getLowerLeft().getLatitude() && point.getLatitude() < boundingBox.getUpperRight().getLatitude()) {
            return true;
        }

        return false;
    }

    public SpatialBoundingBox extendBoundingBoxByPoint(SpatialBoundingBox boundingBox, Point point) {
        String newXMarker;
        String newYMarker;

        if (point.getLongitude() < boundingBox.getLowerLeft().getLongitude()) {
            newXMarker = "less";
        } else if (point.getLongitude() > boundingBox.getUpperRight().getLongitude()) {
            newXMarker = "more";
        } else {
            newXMarker = "middle";
        }

        if (point.getLatitude() < boundingBox.getLowerLeft().getLatitude()) {
            newYMarker = "less";
        } else if (point.getLatitude() > boundingBox.getUpperRight().getLatitude()) {
            newYMarker = "more";
        } else {
            newYMarker = "middle";
        }

        if (newXMarker.equals("less") && newYMarker.equals("less")) {
            return new SpatialBoundingBox(point, boundingBox.getUpperRight());
        } else if (newXMarker.equals("less") && newYMarker.equals("middle")) {
            return new SpatialBoundingBox(new Point(point.getLongitude(), boundingBox.getLowerLeft().getLatitude()), boundingBox.getUpperRight());
        } else if (newXMarker.equals("less") && newYMarker.equals("more")) {
            return new SpatialBoundingBox(new Point(point.getLongitude(), boundingBox.getLowerLeft().getLatitude()), new Point(boundingBox.getUpperRight().getLongitude(), point.getLatitude()));
        } else if (newXMarker.equals("middle") && newYMarker.equals("less")) {
            return new SpatialBoundingBox(new Point(boundingBox.getLowerLeft().getLongitude(), point.getLatitude()), boundingBox.getUpperRight());
        } else if (newXMarker.equals("middle") && newYMarker.equals("middle")) {
            return boundingBox;
        } else if (newXMarker.equals("middle") && newYMarker.equals("more")) {
            return new SpatialBoundingBox(boundingBox.getLowerLeft(), new Point(boundingBox.getUpperRight().getLongitude(), point.getLatitude()));
        } else if (newXMarker.equals("more") && newYMarker.equals("less")) {
            return new SpatialBoundingBox(new Point(boundingBox.getLowerLeft().getLongitude(), point.getLatitude()), new Point(point.getLongitude(), boundingBox.getUpperRight().getLatitude()));
        } else if (newXMarker.equals("more") && newYMarker.equals("middle")) {
            return new SpatialBoundingBox(boundingBox.getLowerLeft(), new Point(point.getLongitude(), boundingBox.getUpperRight().getLatitude()));
        } else {
            // more && more
            return new SpatialBoundingBox(boundingBox.getLowerLeft(), point);
        }

    }

    public List<Long> calculateNonOverlappedGrids(SpatialBoundingBox boundingBox1, SpatialBoundingBox boundingBox2) {
        List<Long> gridList1 = toIndexGrids(boundingBox1);
        List<Long> gridList2 = toIndexGrids(boundingBox2);

        List<Long> result;
        if (gridList2.size() > gridList1.size()) {
            gridList2.removeAll(gridList1);
            result = gridList2;
        } else {
            gridList1.removeAll(gridList2);
            result = gridList1;
        }

        return result;
    }

    public SpatialBoundingBox getBoundingBoxByGridId(long gridId) {

        Map<String, Long> resultMap = splitGridId(gridId);
        long levelOneGridId = resultMap.get("level1");
        long levelTwoGridId = resultMap.get("level2");

        Map<String, Integer> levelOneResult = spaceFillingCurve.from2DCurveValue(levelOneGridId);
        Map<String, Integer> levelTwoResult = spaceFillingCurve.from2DCurveValue(levelTwoGridId);

        double lowerLeftLon = levelOneResult.get("x") * levelOneGridLongitudeWidth + levelTwoResult.get("x") * levelTwoGridLongitudeWidth;
        double upperRightLon = lowerLeftLon + levelTwoGridLongitudeWidth;

        double lowerLeftLat = levelOneResult.get("y") * levelOneGridLatitudeWidth + levelTwoResult.get("y") * levelTwoGridLatitudeWidth;
        double upperRightLat = lowerLeftLat + levelTwoGridLatitudeWidth;

        return new SpatialBoundingBox(new Point(lowerLeftLon, lowerLeftLat), new Point(upperRightLon, upperRightLat));
    }

    public long toCombinedGridIdentifier(long levelOneGridId, long levelTwoGridId) {
        int lonGridNumInLevelTwo = (int) Math.ceil(levelOneGridLongitudeWidth / levelTwoGridLongitudeWidth);
        int latGridNumInLevelTwo = (int) Math.ceil(levelOneGridLatitudeWidth / levelTwoGridLatitudeWidth);
        int validBitNumForLevelTwoGridId = 2 * (int) Math.ceil(Math.max(Math.log(lonGridNumInLevelTwo) / Math.log(2), Math.log(latGridNumInLevelTwo) / Math.log(2)));

        long combinedId = (levelOneGridId << validBitNumForLevelTwoGridId) | levelTwoGridId;
        return combinedId;
    }

    public Map<String, Long> splitGridId(long gridId) {
        HashMap<String, Long> resultMap = new HashMap<>();

        int lonGridNumInLevelTwo = (int) Math.ceil(levelOneGridLongitudeWidth / levelTwoGridLongitudeWidth);
        int latGridNumInLevelTwo = (int) Math.ceil(levelOneGridLatitudeWidth / levelTwoGridLatitudeWidth);
        int validBitNumForLevelTwoGridId = 2 * (int) Math.ceil(Math.max(Math.log(lonGridNumInLevelTwo) / Math.log(2), Math.log(latGridNumInLevelTwo) / Math.log(2)));

        long levelOneGridId = ((gridId & ~MASKS[validBitNumForLevelTwoGridId]) >> validBitNumForLevelTwoGridId);
        long levelTwoGridId = (long)(gridId & MASKS[validBitNumForLevelTwoGridId]);

        resultMap.put("level1", levelOneGridId);
        resultMap.put("level2", levelTwoGridId);

        return resultMap;

    }

    private static final long[] MASKS = new long[] { 0x0000000000000000L, 0x0000000000000001L, 0x0000000000000003L,
            0x0000000000000007L, 0x000000000000000FL, 0x000000000000001FL, 0x000000000000003FL, 0x000000000000007FL,
            0x00000000000000FFL, 0x00000000000001FFL, 0x00000000000003FFL, 0x00000000000007FFL, 0x0000000000000FFFL,
            0x0000000000001FFFL, 0x0000000000003FFFL, 0x0000000000007FFFL, 0x000000000000FFFFL};


    public SpatialBoundingBox getBoundingBoxByLevelOneGridCoordinate(int coordinateLon, int coordinateLat) {
        Point lowerLeft = new Point(coordinateLon * levelOneGridLongitudeWidth, coordinateLat * levelOneGridLatitudeWidth);
        Point upperRight = new Point((coordinateLon + 1) * levelOneGridLongitudeWidth, (coordinateLat + 1) * levelOneGridLatitudeWidth);
        return new SpatialBoundingBox(lowerLeft, upperRight);
    }

    public static SpatialBoundingBox getOverlappedBoundingBox(SpatialBoundingBox boundingBox1, SpatialBoundingBox boundingBox2) {
        Point lowerLeftPoint1 = boundingBox1.getLowerLeft();
        Point upperRightPoint1 = boundingBox1.getUpperRight();

        Point lowerLeftPoint2 = boundingBox2.getLowerLeft();
        Point upperRightPoint2 = boundingBox2.getUpperRight();

        SpatialRange overlappedRangeX = getOverlappedRange(new SpatialRange(lowerLeftPoint1.getLongitude(), upperRightPoint1.getLongitude()), new SpatialRange(lowerLeftPoint2.getLongitude(), upperRightPoint2.getLongitude()));
        SpatialRange overlappedRangeY = getOverlappedRange(new SpatialRange(lowerLeftPoint1.getLatitude(), upperRightPoint1.getLatitude()), new SpatialRange(lowerLeftPoint2.getLatitude(), upperRightPoint2.getLatitude()));

        if (overlappedRangeX != null && overlappedRangeY != null) {
            Point overlappedLowerLeftPoint = new Point(overlappedRangeX.getMin(), overlappedRangeY.getMin());
            Point overlappedUpperRightPoint = new Point(overlappedRangeX.getMax(), overlappedRangeY.getMax());
            return new SpatialBoundingBox(overlappedLowerLeftPoint, overlappedUpperRightPoint);
        }

        return null;
    }

    public static SpatialRange getOverlappedRange(SpatialRange range1, SpatialRange range2) {

        if (range1.getMax() >= range2.getMin() && range1.getMin() <=range2.getMax()) {
            Double overlappedMin = Math.max(range1.getMin(), range2.getMin());
            Double overlappedMax = Math.min(range1.getMax(), range2.getMax());
            return new SpatialRange(overlappedMin, overlappedMax);
        } else {
            return null;
        }
    }
}
