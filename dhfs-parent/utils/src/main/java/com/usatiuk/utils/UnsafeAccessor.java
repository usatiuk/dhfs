package com.usatiuk.utils;

import jdk.internal.access.JavaNioAccess;
import jdk.internal.access.SharedSecrets;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeAccessor {
    private static final UnsafeAccessor INSTANCE;

    static {
        try {
            INSTANCE = new UnsafeAccessor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static UnsafeAccessor get() {
        return INSTANCE;
    }

    private JavaNioAccess _nioAccess;
    private Unsafe _unsafe;

    private UnsafeAccessor() throws NoSuchFieldException, IllegalAccessException {
        _nioAccess = SharedSecrets.getJavaNioAccess();
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        _unsafe = (Unsafe) f.get(null);
    }

    public JavaNioAccess getNioAccess() {
        return _nioAccess;
    }

    public Unsafe getUnsafe() {
        return _unsafe;
    }
}
