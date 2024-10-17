package com.usatiuk.dhfs.supportlib;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class DhfsSupport {
    public static final int PAGE_SIZE;
    private static final Logger LOGGER = Logger.getLogger(DhfsSupport.class.getName());
    private static final DhfsSupportImpl IMPLEMENTATION;

    static {
        DhfsSupportImpl tmp;
        try {
            System.load(DhfsNativeLibFinder.getLibPath().toAbsolutePath().toString());
            tmp = new DhfsSupportImplNative();
        } catch (Throwable e) {
            LOGGER.warning("Failed to load native libraries, using fallback: \n" + e);
            tmp = new DhfsSupportImplFallback();
        }
        IMPLEMENTATION = tmp;
        PAGE_SIZE = getPageSizeInternal();
    }

    static long allocateUninitializedByteBuffer(ByteBuffer[] bb, int size) {
        return IMPLEMENTATION.allocateUninitializedByteBuffer(bb, size);
    }

    static void releaseByteBuffer(long token) {
        IMPLEMENTATION.releaseByteBuffer(token);
    }

    private static int getPageSizeInternal() {
        return IMPLEMENTATION.getPageSizeInternal();
    }
}
