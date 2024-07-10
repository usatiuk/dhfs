package com.usatiuk.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executors;

public class HashSetDelayedBlockingQueueTest {

    @Test
    void Get() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        queue.add("hello!");
        var thing = queue.get();
        var gotTime = System.currentTimeMillis();
        Assertions.assertEquals("hello!", thing);
        Assertions.assertTrue((gotTime - curTime) >= 1000);
    }


    @Test
    void GetAll() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        var ex = Executors.newSingleThreadExecutor();
        ex.submit(() -> {
            try {
                Thread.sleep(10);
                queue.add("hello1");
                queue.add("hello2");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        var thing = queue.getAllWait(); // Theoretically you can get one...
        if (thing.size() == 1) thing.add(queue.getAllWait());
        var gotTime = System.currentTimeMillis();
        Assertions.assertIterableEquals(List.of("hello1", "hello2"), thing);
        Assertions.assertTrue((gotTime - curTime) >= 1010);
    }

}
