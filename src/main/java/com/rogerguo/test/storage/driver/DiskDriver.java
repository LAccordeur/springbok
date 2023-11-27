package com.rogerguo.test.storage.driver;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Date 2021/4/25 11:20
 * @Created by GUO Yang
 */
public class DiskDriver implements PersistenceDriver {

    private String rootUri;  // path uri, used to construct uri for specific object

    public DiskDriver(String rootUri) {
        this.rootUri = rootUri;
        File file = new File(rootUri);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * users should construct the full path before call this function
     * @param key
     * @param dataString
     */
    @Override
    public void flush(String key, String dataString) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                file.createNewFile();
                logger.info(key + " file been created.");
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(dataString.getBytes());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flushBatch(Map<String, String> keyValueMap) {
        for (String key : keyValueMap.keySet()) {
            flush(key, keyValueMap.get(key));
        }
    }

    @Override
    public void flush(String key, String dataString, Map<String, String> metadataMap) {

    }

    @Override
    public Map<String, String> getMetaDataAsString(String key) {
        return null;
    }

    @Override
    public List<String> listKeysWithSamePrefix(String prefix) {
        File folder = new File(prefix);
        File[] listOfFiles = folder.listFiles();

        List<String> keyList = new ArrayList<>();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                //System.out.println("File " + listOfFiles[i].getName());
                String filename = prefix + File.separator + listOfFiles[i].getName();
                keyList.add(filename);
            }
        }

        return keyList;
    }

    @Override
    public List<String> listKeysInBuckets() {
        return null;
    }

    @Override
    public Map<String, String> getDataAsStringBatch(List<String> ketList) {
        Map<String, String> dataMap = new HashMap<>();
        for (String key : ketList) {
            String value = getDataAsString(key);
            dataMap.put(key, value);
        }
        return dataMap;
    }

    @Override
    public Map<String, String> getDataAsStringWithRangeBatch(List<Triple<String, Integer, Integer>> keyList) {
        return null;
    }

    @Override
    public List<String> getDataAsStringWithRangeWithDuplicatedKeyBatch(List<Triple<String, Integer, Integer>> keyList) {
        return null;
    }

    @Override
    public void flush(String key, byte[] dataBytes) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                file.createNewFile();
                logger.info(key + " file been created.");
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(dataBytes);
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getDataAsString(String key) {
        File file = new File(key);

        if (!file.exists()) {
            logger.info("file not exist");
            return "";
        }
        long length = file.length();
        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Not support such large file now");
        }
        int lengthInt = (int) length;
        return getDataAsStringPartial(key, 0, lengthInt);
    }

    @Override
    public byte[] getDataAsByteArray(String key) {
        File file = new File(key);

        if (!file.exists()) {
            logger.info("file not exist");
            return new byte[0];
        }
        long length = file.length();
        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Not support such large file now");
        }
        int lengthInt = (int) length;
        return getDataAsByteArrayPartial(key, 0, lengthInt);
    }

    /**
     * key is file path
     * @param key
     * @param offset  the start point of reading in source file
     * @param length
     * @return
     */
    @Override
    public String getDataAsStringPartial(String key, int offset, int length) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                logger.info("file not exist");
                return "";
            }

            /*FileInputStream fileInputStream = new FileInputStream(file);
            byte[] buffer = new byte[length];
            fileInputStream.getChannel().position();
            fileInputStream.read();*/

            byte[] buffer = new byte[length];
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer, 0, length);
            randomAccessFile.close();
            return new String(buffer);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return "";
    }

    @Override
    public byte[] getDataAsByteArrayPartial(String key, int offset, int length) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                logger.info("file not exist");
                return new byte[0];
            }


            byte[] buffer = new byte[length];
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer, 0, length);
            randomAccessFile.close();
            return buffer;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    public int getFileSize(String filename) {
        File file = new File(filename);
        if (file.exists()) {
            long fileSize = file.length();
            if (fileSize > Integer.MAX_VALUE) {
                throw new RuntimeException("Not support such large file now");
            }

            return (int) fileSize;
        } else {
            logger.info("file [{}] not exist", filename);
            return 0;
        }
    }

    @Override
    public String getRootUri() {
        return this.rootUri;
    }

    @Override
    public void remove(String key) {
        File file = new File(key);

        if (!file.exists()) {
            logger.info("file not exist");
        }
        file.delete();
    }

    @Override
    public void close() {
        // do nothing for disk driver
    }

    @Override
    public long getObjectSize(String key) {
        // not used for disk driver
        return 0;
    }
}
