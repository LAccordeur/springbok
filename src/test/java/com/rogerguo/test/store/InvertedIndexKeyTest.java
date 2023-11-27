package com.rogerguo.test.store;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InvertedIndexKeyTest {

    @Test
    public void generateInvertedIndex() {

        Map<InvertedIndexKey, String> invertedIndexKeyMap = new HashMap<>();
        InvertedIndexKey invertedIndexKey = new InvertedIndexKey(2L,(byte)1);
        invertedIndexKeyMap.put(invertedIndexKey, "value1");

        System.out.println(invertedIndexKeyMap.get(invertedIndexKey));

        InvertedIndexKey invertedIndexKey1 = new InvertedIndexKey(2L, (byte)1);
        System.out.println(invertedIndexKeyMap.get(invertedIndexKey1));

    }
}