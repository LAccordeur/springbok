package com.rogerguo.test.store;

import com.rogerguo.test.storage.driver.DiskDriver;
import org.junit.Test;

import java.io.File;

public class ImmutableMemoryRegionTest {

    DiskDriver diskDriver = new DiskDriver("data-test");
    ImmutableMemoryRegion immutableMemoryRegion = new ImmutableMemoryRegion(diskDriver);


    @Test
    public void moveToImmutableMemoryRegion() {
        // a new chunk for a new trajectory
        Chunk chunk = new Chunk("device_1", "1111");
        immutableMemoryRegion.moveToImmutableMemoryRegion("device_1", chunk);
        System.out.println(immutableMemoryRegion);

        // a new chunk of the same trajectory
        Chunk chunk1 = new Chunk("device_1", "2222");
        immutableMemoryRegion.moveToImmutableMemoryRegion("device_1", chunk1);
        System.out.println(immutableMemoryRegion);

        Chunk chunk2 = new Chunk("device_2", "1111");
        immutableMemoryRegion.moveToImmutableMemoryRegion("device_2", chunk2);
        System.out.println(immutableMemoryRegion);

        immutableMemoryRegion.flush();

    }

    @Test
    public void flush() {
        System.out.println(File.separator);
    }
}