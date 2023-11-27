package com.rogerguo.test.test;

import java.util.ArrayList;
import java.util.List;

public class OutofmemoryTest {

    static List<long[]> list = new ArrayList<long[]>();
    public static void main(String[] args) {
        while (true) {
            final long[] l = new long[10000];
            list.add(l);
        }
    }

}
