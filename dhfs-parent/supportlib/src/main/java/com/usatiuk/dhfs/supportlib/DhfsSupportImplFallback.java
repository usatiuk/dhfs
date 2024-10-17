package com.usatiuk.dhfs.supportlib;

import java.nio.ByteBuffer;

class DhfsSupportImplFallback implements DhfsSupportImpl {
    @Override
    public long allocateUninitializedByteBuffer(ByteBuffer[] bb, int size) {
        bb[0] = ByteBuffer.allocateDirect(size);
        return -1;
    }

    @Override
    public void releaseByteBuffer(long token) {
        // GC
    }

    @Override
    public int getPageSizeInternal() {
        return 4096; // FIXME:?
    }
}
