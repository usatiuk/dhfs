package com.usatiuk.dhfs.supportlib;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class UninitializedByteBuffer {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final Logger LOGGER = Logger.getLogger(UninitializedByteBuffer.class.getName());

    public static ByteBuffer allocateUninitialized(int size) {
        if (size < DhfsSupportNative.PAGE_SIZE)
            return ByteBuffer.allocateDirect(size);

        var bb = new ByteBuffer[1];
        long token = DhfsSupportNative.allocateUninitializedByteBuffer(bb, size);
        var ret = bb[0];
        CLEANER.register(ret, () -> {
            try {
                DhfsSupportNative.releaseByteBuffer(token);
            } catch (Throwable e) {
                LOGGER.severe("Error releasing buffer: " + e.toString());
                System.exit(-1);
            }
        });
        return ret;
    }
}
