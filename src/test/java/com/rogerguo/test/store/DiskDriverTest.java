package com.rogerguo.test.store;

import com.rogerguo.test.storage.driver.DiskDriver;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class DiskDriverTest {

    @Test
    public void flush() {

        DiskDriver diskDriver = new DiskDriver("data-test");
        String testString = "this is a test string.";
        diskDriver.flush("data-test/test.file", testString);

    }

    @Test
    public void getDataAsString() {
        DiskDriver diskDriver = new DiskDriver("data-test");
        String result = diskDriver.getDataAsStringPartial("data-test/test.file", 0, 4);
        System.out.println(result);
    }

    @Test
    public void flushBytes() {
        DiskDriver diskDriver = new DiskDriver("data-test");
        String testString = "this is a test string.";
        diskDriver.flush("data-test/test-byte.file", testString.getBytes(StandardCharsets.UTF_8));

    }

    @Test
    public void delete() {
        DiskDriver diskDriver = new DiskDriver("data-test");
        diskDriver.remove("data-test/test-byte.file");
    }
}