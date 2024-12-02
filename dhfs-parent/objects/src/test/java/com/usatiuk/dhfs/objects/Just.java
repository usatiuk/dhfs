package com.usatiuk.dhfs.objects;

import java.util.concurrent.Callable;

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
}
