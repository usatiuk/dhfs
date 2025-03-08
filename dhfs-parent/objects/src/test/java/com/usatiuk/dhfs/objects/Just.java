package com.usatiuk.dhfs.objects;

import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public abstract class Just {
    public static void run(Callable<?> callable) {
        new Thread(() -> {
            try {
                callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    public static void runAll(Callable<?>... callables) {
        try {
            try (var exs = Executors.newFixedThreadPool(callables.length)) {
                exs.invokeAll(Arrays.stream(callables).map(c -> (Callable<?>) () -> {
                    try {
                        return c.call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toList()).forEach(f -> {
                    try {
                        f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void runAll(Runnable... callables) {
        try {
            try (var exs = Executors.newFixedThreadPool(callables.length)) {
                exs.invokeAll(Arrays.stream(callables).map(c -> (Callable<?>) () -> {
                    try {
                        c.run();
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toList()).forEach(f -> {
                    try {
                        f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <K> void checkIterator(Iterator<K> it, List<K> expected) {
        for (var e : expected) {
            Assertions.assertTrue(it.hasNext());
            var next = it.next();
            Assertions.assertEquals(e, next);
        }
    }

    @SafeVarargs
    public static <K> void checkIterator(Iterator<K> it, K... expected) {
        checkIterator(it, Arrays.asList(expected));
    }
}
