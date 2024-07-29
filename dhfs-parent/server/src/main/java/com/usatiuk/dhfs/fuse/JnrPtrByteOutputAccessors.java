package com.usatiuk.dhfs.fuse;

import jakarta.inject.Singleton;
import jdk.internal.access.JavaNioAccess;
import jdk.internal.access.SharedSecrets;
import lombok.Getter;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

@Singleton
class JnrPtrByteOutputAccessors {
    @Getter
    JavaNioAccess _nioAccess;
    @Getter
    Unsafe _unsafe;

    JnrPtrByteOutputAccessors() throws NoSuchFieldException, IllegalAccessException {
        _nioAccess = SharedSecrets.getJavaNioAccess();
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        _unsafe = (Unsafe) f.get(null);
    }
}
