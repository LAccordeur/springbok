package com.rogerguo.test.storage.flush;

import com.rogerguo.test.storage.Block;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class S3SpatioTemporalSTRLayoutSchemaToolTest {

    @Test
    public void generateQueueList() {

        List<String> blockIds = generateSyntheticBlockIds2();
        System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.generateQueueList(blockIds, 2, 2));

    }

    @Test
    public void assembleBlocksForSpatioTemporalLayout() {
        List<Block> blockList = generateSyntheticBlocks();
        Block block = S3SpatioTemporalSTRLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockList, new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, 10));
        System.out.println(block);

    }

    private List<Block> generateSyntheticBlocks() {

        List<Block> blockList = new ArrayList<>();

        Block block1 = new Block("T000.100.64.1", "data1");
        blockList.add(block1);
        Block block2 = new Block("T000.100.64.2", "data2");
        blockList.add(block2);
        Block block3 = new Block("T000.101.64.1", "data3");
        blockList.add(block3);

        return blockList;
    }

    private List<String> generateSyntheticBlockIds2() {
        List<String> blockIdList = new ArrayList<>();

        /*String id1 = "T001.1.0.0";
        blockIdList.add(id1);
        String id2 = "T001.1.0.1";
        blockIdList.add(id2);*/
        String id3 = "T001.2.1.0";
        blockIdList.add(id3);
        /*String id4 = "T002.1.3.0";
        blockIdList.add(id4);*/
        String id5 = "T002.2.6.0";
        blockIdList.add(id5);
        String id6 = "T002.3.6.0";
        blockIdList.add(id6);

        return blockIdList;
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
    public void generateObjectKeyForPut() {
        for (int i = 0; i < 4; i++) {
            System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.generateObjectKeyForPut(23));
        }
        for (int i = 0; i < 4; i++) {
            System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.generateObjectKeyForPut(24));
        }

    }

    @Test
    public void generateMetaDataObjectKeyForQuery() {
        String blockId = "T000.100000.16.0";
        System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, 100));


    }
}