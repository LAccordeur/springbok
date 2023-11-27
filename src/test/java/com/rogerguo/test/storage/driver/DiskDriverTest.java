package com.rogerguo.test.storage.driver;

import org.junit.Test;

import java.util.Random;

/**
 * @author yangguo
 * @create 2022-10-08 2:23 PM
 **/
public class DiskDriverTest {

    @Test
    public void testRead() {
        Random random = new Random(1);
        DiskDriver diskDriver = new DiskDriver("/home/yangguo/IdeaProjects/trajectory-index/flush-test");
        long start = System.currentTimeMillis();
        /*for (int i = 0; i < 2000; i++) {
            diskDriver.getDataAsStringPartial("/home/yangguo/IdeaProjects/trajectory-index/flush-test/trajectory.data.1", random.nextInt(1024 * 1024 * 1024), 4096);
        }*/
        diskDriver.getDataAsStringPartial("/home/yangguo/IdeaProjects/trajectory-index/flush-test/trajectory.data.1", 2, 4096 * 1000);
        long stop = System.currentTimeMillis();
        System.out.println("time: " + (stop - start));

    }

}
