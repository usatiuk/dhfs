package com.usatiuk.dhfs.supportlib;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;

public class UninitializedByteBuffer {
    private static final Cleaner CLEANER = Cleaner.create();

    public static ByteBuffer allocateUninitialized(int size) {
        if (size < DhfsSupportNative.PAGE_SIZE)
            return ByteBuffer.allocateDirect(size);

        var bb = new ByteBuffer[1];
        long token = DhfsSupportNative.allocateUninitializedByteBuffer(bb, size);
        var ret = bb[0];
        CLEANER.register(ret, () -> DhfsSupportNative.releaseByteBuffer(token));
        return ret;
    }
}
