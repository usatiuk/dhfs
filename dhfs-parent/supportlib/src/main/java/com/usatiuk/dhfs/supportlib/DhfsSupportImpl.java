package com.usatiuk.dhfs.supportlib;

import java.nio.ByteBuffer;

interface DhfsSupportImpl {
    long allocateUninitializedByteBuffer(ByteBuffer[] bb, int size);

    void releaseByteBuffer(long token);

    int getPageSizeInternal();
}
