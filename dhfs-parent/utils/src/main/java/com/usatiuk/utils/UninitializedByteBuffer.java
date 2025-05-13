package com.usatiuk.utils;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Utility class for creating uninitialized ByteBuffers, to avoid zeroing memory unnecessarily.
 */
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

    /**
     * Allocates a new uninitialized ByteBuffer of the specified capacity.
     *
     * @param capacity the capacity of the ByteBuffer
     * @return a new uninitialized ByteBuffer
     */
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

    /**
     * Gets the address of the given ByteBuffer.
     *
     * @param buffer the ByteBuffer to get the address of
     * @return the address of the ByteBuffer
     */
    public static long getAddress(ByteBuffer buffer) {
        return UnsafeAccessor.NIO.getBufferAddress(buffer);
    }
}
