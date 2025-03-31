package com.usatiuk.dhfs.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
    void addNoDelay() throws InterruptedException {
        var queue = new HashSetDelayedBlockingQueue<>(1000);

        var curTime = System.currentTimeMillis();
        queue.addNoDelay("hello!");
        var thing = queue.get();
        var gotTime = System.currentTimeMillis();
        Assertions.assertEquals("hello!", thing);
        Assertions.assertTrue((gotTime - curTime) < 500);
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
        if (thing.size() == 1) thing.addAll(queue.getAllWait());
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
        Thread.sleep(500);
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
                queue.readd("hello1");
                queue.readd("hello2");
                Thread.sleep(800);
                queue.readd("hello1");
                queue.readd("hello2");
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
        AtomicBoolean ok = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            Assertions.assertThrows(InterruptedException.class, queue::get);
            Assertions.assertThrows(InterruptedException.class, queue::getAllWait);
            Assertions.assertTrue((System.currentTimeMillis() - curTime) < 2000);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ok.set(true);
        });

        t.start();

        Thread.sleep(500);
        t.interrupt();
        Thread.sleep(500);
        t.interrupt();

        Thread.sleep(1500);

        Assertions.assertTrue(ok.get());
    }

    @Test
    void setDelayTest() throws InterruptedException, ExecutionException, TimeoutException {
        var queue = new HashSetDelayedBlockingQueue<String>(100000);

        var curTime = System.currentTimeMillis();
        var ex = Executors.newSingleThreadExecutor();

        var future = ex.submit(() -> {
            Assertions.assertEquals("hello1", queue.get());
            Assertions.assertTrue((System.currentTimeMillis() - curTime) < 2000);
            var startTime2 = System.currentTimeMillis();
            Assertions.assertEquals("hello2", queue.get());
            Assertions.assertTrue((System.currentTimeMillis() - startTime2) < 200);
            var startTime3 = System.currentTimeMillis();
            Assertions.assertEquals("hello3", queue.get());
            Assertions.assertTrue((System.currentTimeMillis() - startTime3) >= 1000);
            return null;
        });

        Thread.sleep(500);
        queue.add("hello1");
        queue.add("hello2");
        Thread.sleep(500);
        queue.setDelay(0);
        Thread.sleep(500);
        queue.setDelay(1000);
        queue.add("hello3");

        future.get(10, TimeUnit.SECONDS);
    }

}
