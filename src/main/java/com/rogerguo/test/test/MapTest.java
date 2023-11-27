package com.rogerguo.test.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-09-27 9:27 PM
 **/
public class MapTest {

    public static void main(String[] args) {
        Map<String, List<String>> map = new HashMap<>();
        Mapping mapping = new Mapping(new String("block1"), new String("file1"));
        Mapping mapping1 = new Mapping(new String("block2"), new String("file1"));
        map.put("file1", new ArrayList<>());
        map.get(mapping1.getFilepath()).add(mapping1.getBlockId());
        map.get(mapping.getFilepath()).add(mapping.getBlockId());

        Map<String, String> test = new HashMap();
        test.put("key", "1");
        test.put("key", "2");

        System.out.println(test);
    }

}


class Mapping {
    String blockId;

    String filepath;

    @Override
    public String toString() {
        return "Mapping{" +
                "blockId='" + blockId + '\'' +
                ", filepath='" + filepath + '\'' +
                '}';
    }

    public Mapping(String blockId, String filepath) {
        this.blockId = blockId;
        this.filepath = filepath;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }
}