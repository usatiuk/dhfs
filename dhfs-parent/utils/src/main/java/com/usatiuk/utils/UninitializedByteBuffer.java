package com.usatiuk.utils;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class UninitializedByteBuffer {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final MethodHandle malloc = LINKER.downcallHandle(
            LINKER.defaultLookup().find("malloc").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
    );
    private static final MethodHandle free = LINKER.downcallHandle(
            LINKER.defaultLookup().find("free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    );

    public static ByteBuffer allocate(int capacity) {
        UnsafeAccessor.NIO.reserveMemory(capacity, capacity);

        MemorySegment segment = null;
        try {
            segment = (MemorySegment) malloc.invokeExact((long) capacity);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        Consumer<MemorySegment> cleanup = s -> {
            try {
                free.invokeExact(s);
                UnsafeAccessor.NIO.unreserveMemory(capacity, capacity);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
        var reint = segment.reinterpret(capacity, Arena.ofAuto(), cleanup);
        return reint.asByteBuffer();
    }

    public static long getAddress(ByteBuffer buffer) {
        return UnsafeAccessor.NIO.getBufferAddress(buffer);
    }
}
