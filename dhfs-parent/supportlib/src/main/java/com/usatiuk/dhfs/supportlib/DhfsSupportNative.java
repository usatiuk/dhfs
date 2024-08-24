package com.usatiuk.dhfs.supportlib;


import java.nio.ByteBuffer;


public class DhfsSupportNative {
    static public final int PAGE_SIZE;

    static {
        System.load(DhfsNativeLibFinder.getLibPath().toAbsolutePath().toString());
        PAGE_SIZE = getPageSizeInternal();
    }

    public static native void hello();

    static native long allocateUninitializedByteBuffer(ByteBuffer[] bb, int size);

    static native void releaseByteBuffer(long token);

    private static native int getPageSizeInternal();
}