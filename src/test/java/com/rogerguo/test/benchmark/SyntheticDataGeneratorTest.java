package com.rogerguo.test.benchmark;

import org.junit.Test;

import static org.junit.Assert.*;

public class SyntheticDataGeneratorTest {

    @Test
    public void nextRandomTrajectoryPoint() {

        for (int i = 0; i < 100; i++) {
            System.out.println(SyntheticDataGenerator.nextRandomTrajectoryPoint());
        }

    }
}