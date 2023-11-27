package com.rogerguo.test.util;

import com.rogerguo.test.motivation.PutObjects;
import com.rogerguo.test.storage.Block;
import com.rogerguo.test.store.Chunk;

import java.util.Random;

/**
 * @author yangguo
 * @create 2021-09-28 4:33 PM
 **/
public class BlockGenerator {

    public static int blockCount = 0;

    public static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(generateNextBlock());
        }
    }

    public static Block generateNextBlock() {
        String dataString = PutObjects.generateRandomString(16);
        Block block = new Block();
        long randomTime = Math.abs(random.nextLong());
        long randomSpatial = Math.abs(random.nextLong());
        block.setBlockId("E000." + randomTime + "." + randomSpatial + "." + blockCount);
        block.setDataString(dataString);
        blockCount++;
        return block;
    }

}
