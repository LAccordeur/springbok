package com.rogerguo.test.benchmark;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.store.*;
import org.openjdk.jmh.infra.Blackhole;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author yangguo
 * @create 2021-12-08 4:27 PM
 **/
public class Test {

    public long listSize = 1000000;

    public List<Chunk> deletionList = new ArrayList<>();

    public SeriesStore seriesStore = SeriesStore.initNewStoreForInMemTest();

    public HeadChunkIndexWithRtree headChunkIndexWithRtree = new HeadChunkIndexWithRtree();

    public HeadChunkIndexWithRtreeMBR headChunkIndexWithRtreeMBR = new HeadChunkIndexWithRtreeMBR();

    public HeadChunkIndexWithGeoHash headChunkIndexWithGeoHash = new HeadChunkIndexWithGeoHash();

    public HeadChunkIndexWithGeoHashSemiSplit headChunkIndexWithGeoHashSemiSplit = new HeadChunkIndexWithGeoHashSemiSplit();

    public HeadChunkIndexWithGeoHashPhysicalSplit headChunkIndexWithGeoHashPhysicalSplit = new HeadChunkIndexWithGeoHashPhysicalSplit(seriesStore);


    public void setup() {
        System.out.println("setup state");
        Random random = new Random();
        for (long i = 0; i < listSize; i++) {
            TrajectoryPoint point = new TrajectoryPoint(String.valueOf(i), 1, random.nextDouble()*180, random.nextDouble()*90);
            seriesStore.appendSeriesPoint(point);
            headChunkIndexWithRtree.updateIndex(point);
            headChunkIndexWithRtreeMBR.updateIndex(point);
            headChunkIndexWithGeoHash.updateIndex(point);
            headChunkIndexWithGeoHashSemiSplit.updateIndex(point);
            headChunkIndexWithGeoHashPhysicalSplit.updateIndex(point);

            if (deletionList.size() < 100000) {
                Chunk chunk = new Chunk(String.valueOf(i));
                List<TrajectoryPoint> pointList = new ArrayList<>();
                pointList.add(point);
                chunk.setChunk(pointList);
                deletionList.add(chunk);
            }
        }

    }

    public void geoHashPhysicalSplitDeletion() {
        for (Chunk chunk : deletionList) {
            headChunkIndexWithGeoHash.removeFromIndex(chunk);
        }
    }

    public static void main(String[] args) throws ParseException {
        /*Test test = new Test();
        test.setup();
        test.geoHashPhysicalSplitDeletion();*/
        List<TrajectoryPoint> trajectoryPoints = SyntheticDataGenerator.generateGaussianDistributionDataSet(10, 1, 1);
        Random random = new Random(1);
        for (int i = 0; i < 10; i++) {
            int index = random.nextInt(trajectoryPoints.size());
            TrajectoryPoint point = trajectoryPoints.get(index);

            double xLow = point.getLongitude();
            double xHigh = xLow + 1 * 0.1;
            double yLow = point.getLatitude();
            double yHigh = yLow + 1 * 0.1;
            System.out.println(new SpatialBoundingBox(new Point(xLow, yLow), new Point(xHigh, yHigh)));
        }

    }
}
