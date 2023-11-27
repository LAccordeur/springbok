package com.rogerguo.test.motivation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author yangguo
 * @create 2021-09-28 11:06 AM
 **/
public class SaveStatisticUtils {

    public static void saveResultToFile(String dataString, String filename) {
        File file = new File(filename);

        try {
            if (!file.exists()) {
                file.createNewFile();
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(dataString.getBytes());
            fileOutputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
