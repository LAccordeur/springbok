package com.rogerguo.test.test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yangguo
 * @create 2021-09-20 3:32 PM
 **/
public class CommonTest {

    public static void main(String[] args) {
        Parent child = new Child(11, 11);
        Parent child2 = new ChildTwo(22);
        child2.function();
    }

    public static void testMapRemove() {
        Map<String, String> hashMap = new HashMap<>();
        hashMap.put("k1", "v1");
        hashMap.put("k2", "v2");
        hashMap.put("k3", "v3");
        System.out.println("before removing");
        System.out.println(hashMap);
        System.out.println("after removing");
        hashMap.clear();
        System.out.println(hashMap);
        System.out.println("finish");
    }

}

class Parent {
    int name;

    public Parent(int name) {
        this.name = name;
    }

    public void print() {
        System.out.println("parent");
    }

    public void function() {
        print();
    }
}

class Child extends Parent {
    int age;

    public Child(int name, int age) {
        super(name);
        this.age = age;
    }

    @Override
    public String toString() {
        return "Child{" +
                "name=" + name +
                ", age=" + age +
                '}';
    }

    @Override
    public void print() {
        System.out.println("child");
    }
}

class ChildTwo extends Parent {
    public ChildTwo(int name) {
        super(name);
    }

    @Override
    public void print() {
        System.out.println("Child Two");
    }
}