package com.usatiuk.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    void GetImmediate() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(0);

        var curTime = System.currentTimeMillis();
        queue.add("hello!");
        Assertions.assertEquals("hello!", queue.get());
        var gotTime = System.currentTimeMillis();
        Assertions.assertTrue((gotTime - curTime) <= 10);
    }

    @Test
    void GetTimeout() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        var thing = queue.get(500L);
        Assertions.assertNull(thing);
        var gotTime = System.currentTimeMillis();
        Assertions.assertTrue((gotTime - curTime) <= 10000);
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


    @Test
    void GetAllLimit() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        var ex = Executors.newSingleThreadExecutor();
        ex.submit(() -> {
            try {
                Thread.sleep(10);
                queue.add("hello1");
                queue.add("hello2");
                queue.add("hello3");
                queue.add("hello4");
                queue.add("hello5");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(100);
        var got1 = queue.getAllWait(3);
        var got2 = queue.getAllWait(3);
        Assertions.assertEquals(3, got1.size());
        Assertions.assertEquals(2, got2.size());
        var gotTime = System.currentTimeMillis();
        Assertions.assertIterableEquals(List.of("hello1", "hello2", "hello3"), got1);
        Assertions.assertIterableEquals(List.of("hello4", "hello5"), got2);
        Assertions.assertTrue((gotTime - curTime) >= 1010);
    }

    @Test
    void GetAllLimitImmediate() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        var ex = Executors.newSingleThreadExecutor();
        ex.submit(() -> {
            try {
                Thread.sleep(10);
                queue.add("hello1");
                queue.add("hello2");
                queue.add("hello3");
                queue.add("hello4");
                queue.add("hello5");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(1100);
        var got1 = queue.getAllWait(3);
        var got2 = queue.getAllWait(3);
        Assertions.assertEquals(3, got1.size());
        Assertions.assertEquals(2, got2.size());
        var gotTime = System.currentTimeMillis();
        Assertions.assertIterableEquals(List.of("hello1", "hello2", "hello3"), got1);
        Assertions.assertIterableEquals(List.of("hello4", "hello5"), got2);
    }

    @Test
    void readdTest() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        var ex = Executors.newSingleThreadExecutor();
        ex.submit(() -> {
            try {
                Thread.sleep(10);
                queue.add("hello1");
                queue.add("hello2");
                Thread.sleep(800);
                queue.add("hello1");
                queue.add("hello2");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        var thing = queue.getAllWait(); // Theoretically you can get one...
        if (thing.size() == 1) thing.add(queue.getAllWait().stream().findFirst().get());
        var gotTime = System.currentTimeMillis();
        Assertions.assertIterableEquals(List.of("hello1", "hello2"), thing);
        Assertions.assertTrue((gotTime - curTime) >= 1810);
    }

    @Test
    void interruptTest() throws InterruptedException, ExecutionException, TimeoutException {
        var queue = new HashSetDelayedBlockingQueue<>(100000);

        var curTime = System.currentTimeMillis();
        var ex = Executors.newSingleThreadExecutor();

        var future = ex.submit(() -> {
            Assertions.assertThrows(InterruptedException.class, queue::get);
            Assertions.assertThrows(InterruptedException.class, queue::getAllWait);
            Assertions.assertTrue((System.currentTimeMillis() - curTime) < 2000);
            Thread.sleep(1000);
            return null;
        });

        Thread.sleep(500);
        queue.interrupt();
        Thread.sleep(500);
        queue.interrupt();

        future.get(10, TimeUnit.SECONDS);
    }

}
