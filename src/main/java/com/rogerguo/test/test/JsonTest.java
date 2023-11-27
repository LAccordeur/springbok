package com.rogerguo.test.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.storage.BlockLocation;
import com.rogerguo.test.storage.StorageLayerName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-09-27 12:58 PM
 **/
public class JsonTest {

    public static void main(String[] args) {
        testComplexMap();
    }

    public static void testComplexMap() {
        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, String> metadataMap = new HashMap<>();
        BlockLocation blockLocation = new BlockLocation(StorageLayerName.S3, "test", 1, 22);
        try {

            String metadataValueString = objectMapper.writeValueAsString(blockLocation);
            metadataMap.put("key1", metadataValueString);

            String result = objectMapper.writeValueAsString(metadataMap);
            System.out.println(result);
            Map<String, String> map = objectMapper.readValue(result, Map.class);
            System.out.println(objectMapper.readValue(map.get("key1"), BlockLocation.class));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testMap() {
        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");

        try {
            String result = objectMapper.writeValueAsString(testMap);
            System.out.println(result);
            Map<String, String> map = objectMapper.readValue(result, Map.class);
            System.out.println(map.get("key1"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void testToJson() {
        ObjectMapper objectMapper = new ObjectMapper();

        Test test = new Test("student", 11);
        try {
            String result = objectMapper.writeValueAsString(test);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testFromJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        Test test = new Test("student", 11);
        try {
            String result = objectMapper.writeValueAsString(test);
            System.out.println(result);

            Test rebuild = objectMapper.readValue(result, Test.class);
            System.out.println(rebuild);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

class Test {

    private String name;

    private int age;

    public Test() {}

    public Test(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Test{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
