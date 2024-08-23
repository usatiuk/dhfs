package com.usatiuk.dhfs.supportlib;


import java.nio.ByteBuffer;


class DhfsSupportNative {
    static {
        System.load(DhfsNativeLibFinder.getLibPath().toAbsolutePath().toString());
    }

    public static native void hello();

    static native long allocateUninitializedByteBuffer(ByteBuffer[] bb, int size);

    static native void dropByteBuffer(long token);

}