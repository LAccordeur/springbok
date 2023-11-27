package com.rogerguo.test.storage.flush;

import com.rogerguo.test.storage.Block;
import com.rogerguo.test.util.CurveUtil;
import com.rogerguo.test.util.ZCurve;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class S3SpatioTemporalLayoutSchemaToolTest {

    @Test
    public void assembleBlocks() {
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL, 16, 10);
        System.out.println(S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(generateSyntheticBlocks(), s3LayoutSchema));

    }

    private List<Block> generateSyntheticBlocks() {
        List<Block> blockList = new ArrayList<>();
        Block block = new Block("T000.64.100.1", "test1");
        blockList.add(block);
        Block block1 = new Block("T001.64.101.1", "test2");
        blockList.add(block1);
        Block block2 = new Block("T002.64.102.1", "test3");
        blockList.add(block2);
        return blockList;
    }

    @Test
    public void generateQueueList() {

        List<String> blockIdList = generateSyntheticBlockIds();
        System.out.println(S3SpatioTemporalLayoutSchemaTool.generateQueueList(blockIdList, 2, 1));

    }

    private List<String> generateSyntheticBlockIds() {
        List<String> blockIdList = new ArrayList<>();

        String id1 = "T000.64.100.1";
        blockIdList.add(id1);
        String id2 = "T003.64.100.2";
        blockIdList.add(id2);
        String id3 = "T003.64.100.1";
        blockIdList.add(id3);
        String id4 = "T000.64.100.2";
        blockIdList.add(id4);
        String id5 = "T006.64.102.1";
        blockIdList.add(id5);
        String id6 = "T006.64.102.2";
        blockIdList.add(id6);
        String id7 = "T008.64.102.1";
        blockIdList.add(id7);
        String id8 = "T008.64.102.2";
        blockIdList.add(id8);
        String id9 = "T011.64.104.1";
        blockIdList.add(id9);
        String id10 = "T011.64.104.2";
        blockIdList.add(id10);
        String id11 = "T012.64.104.1";
        blockIdList.add(id11);
        String id12 = "T012.64.104.2";
        blockIdList.add(id12);


        return blockIdList;
    }

    @Test
    public void generateTimePartitionId() {

        long timestamp = System.currentTimeMillis();
        System.out.println("timestamp: " + timestamp);
        System.out.println(S3SpatioTemporalLayoutSchemaTool.generateTimePartitionId(timestamp, 1000));
    }

    @Test
    public void generateSpatialPartitionId() {
        ZCurve zCurve = new ZCurve();
        int x = 1;
        int y = 111;
        long encoding = zCurve.getCurveValue(x, y);
        System.out.println("encoding: " + encoding);
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(encoding)));
        long id = S3SpatioTemporalLayoutSchemaTool.generateSpatialPartitionId(encoding, 9);
        System.out.println("id: " + id);
        System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(id)));

    }

    @Test
    public void generateMetaDataObjectKeyForQuery() {

        String blockId = "T000.100000.16.0";
        System.out.println(S3SpatioTemporalLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, 100, 2));

    }

    @Test
    public void generateObjectKeyForPut() {

        for (int i = 0; i < 4; i++) {
            System.out.println(S3SpatioTemporalLayoutSchemaTool.generateObjectKeyForPut(23, 23));
        }
        for (int i = 0; i < 4; i++) {
            System.out.println(S3SpatioTemporalLayoutSchemaTool.generateObjectKeyForPut(24, 23));
        }

    }
}