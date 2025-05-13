package com.usatiuk.utils;

import jdk.internal.access.JavaNioAccess;
import jdk.internal.access.SharedSecrets;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Provides access to the {@link Unsafe} class and {@link JavaNioAccess} class.
 */
public abstract class UnsafeAccessor {
    public static final JavaNioAccess NIO;
    public static final Unsafe UNSAFE;

    static {
        try {
            NIO = SharedSecrets.getJavaNioAccess();
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
