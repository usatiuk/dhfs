package com.usatiuk.dhfs.supportlib;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class UninitializedByteBuffer {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final Logger LOGGER = Logger.getLogger(UninitializedByteBuffer.class.getName());

    public static ByteBuffer allocateUninitialized(int size) {
        try {
            if (size < DhfsSupport.PAGE_SIZE)
                return ByteBuffer.allocateDirect(size);

            var bb = new ByteBuffer[1];
            long token = DhfsSupport.allocateUninitializedByteBuffer(bb, size);
            var ret = bb[0];
            CLEANER.register(ret, () -> {
                try {
                    DhfsSupport.releaseByteBuffer(token);
                } catch (Throwable e) {
                    LOGGER.severe("Error releasing buffer: " + e);
                    System.exit(-1);
                }
            });
            return ret;
        } catch (OutOfMemoryError e) {
            return ByteBuffer.allocate(size);
        }
    }
}
