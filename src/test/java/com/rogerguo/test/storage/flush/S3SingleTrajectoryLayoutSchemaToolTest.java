package com.rogerguo.test.storage.flush;

import com.rogerguo.test.storage.Block;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class S3SingleTrajectoryLayoutSchemaToolTest {

    @Test
    public void generateQueueList() {
        List<String> blockIdList = generateSyntheticBlockIds();
        System.out.println(S3SingleTrajectoryLayoutSchemaTool.generateQueueList(blockIdList, 2));

    }

    private List<String> generateSyntheticBlockIds() {
        List<String> blockIdList = new ArrayList<>();

        String id1 = "T003.64.1";
        blockIdList.add(id1);
        String id2 = "T003.64.2";
        blockIdList.add(id2);
        String id3 = "T003.65.1";
        blockIdList.add(id3);
        String id4 = "T000.64.1";
        blockIdList.add(id4);
        String id5 = "T006.64.1";
        blockIdList.add(id5);
        String id6 = "T006.64.2";
        blockIdList.add(id6);
        String id7 = "T008.64.1";
        blockIdList.add(id7);
        String id8 = "T008.64.2";
        blockIdList.add(id8);
        String id9 = "T011.64.1";
        blockIdList.add(id9);
        String id10 = "T011.64.2";
        blockIdList.add(id10);
        String id11 = "T012.64.1";
        blockIdList.add(id11);
        String id12 = "T012.64.2";
        blockIdList.add(id12);


        return blockIdList;
    }

    @Test
    public void assembleBlocksForSingleTrajectoryLayout() {
        List<Block> blockList = generateSyntheticBlocks();
        Block result = S3SingleTrajectoryLayoutSchemaTool.assembleBlocksForSingleTrajectoryLayout(blockList, new S3LayoutSchema(S3LayoutSchemaName.SINGLE_TRAJECTORY,10));
        System.out.println(result);
    }

    private List<Block> generateSyntheticBlocks() {

        List<Block> blockList = new ArrayList<>();

        Block block1 = new Block("T000.100.1", "data1");
        blockList.add(block1);
        Block block2 = new Block("T000.100.2", "data2");
        blockList.add(block2);
        Block block3 = new Block("T000.101.1", "data3");
        blockList.add(block3);

        return blockList;
    }

    @Test
    public void generateMetaDataObjectKeyForQuery() {

        String blockId = "T011.64.2";
        System.out.println(S3SingleTrajectoryLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, 10));
    }

    @Test
    public void generateObjectKeyForPut() {

        System.out.println(S3SingleTrajectoryLayoutSchemaTool.generateObjectKeyForPut(11, "T001"));
        System.out.println(S3SingleTrajectoryLayoutSchemaTool.generateObjectKeyForPut(11, "T001"));
    }
}