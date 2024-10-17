package com.usatiuk.dhfs.supportlib;

import java.nio.ByteBuffer;

class DhfsSupportImplNative implements DhfsSupportImpl {
    @Override
    public long allocateUninitializedByteBuffer(ByteBuffer[] bb, int size) {
        return DhfsSupportNative.allocateUninitializedByteBuffer(bb, size);
    }

    @Override
    public void releaseByteBuffer(long token) {
        DhfsSupportNative.releaseByteBuffer(token);
    }

    @Override
    public int getPageSizeInternal() {
        return DhfsSupportNative.PAGE_SIZE;
    }
}
