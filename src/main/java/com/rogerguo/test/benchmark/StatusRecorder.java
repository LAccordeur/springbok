package com.rogerguo.test.benchmark;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @author yangguo
 * @create 2021-12-24 11:22 AM
 **/
public class StatusRecorder {

    public static void recordStatus(String filename, String logRecord) {

        FileWriter fileWriter;

        try {
            fileWriter = new FileWriter(filename, true);
            fileWriter.write(logRecord + "\n");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
