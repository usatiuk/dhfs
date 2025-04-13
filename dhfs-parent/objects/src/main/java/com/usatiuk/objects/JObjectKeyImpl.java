package com.usatiuk.objects;

import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record JObjectKeyImpl(String value) implements JObjectKey {
    @Override
    public int compareTo(JObjectKey o) {
        switch (o) {
            case JObjectKeyImpl jObjectKeyImpl -> {
                return value.compareTo(jObjectKeyImpl.value());
            }
            case JObjectKeyMax jObjectKeyMax -> {
                return -1;
            }
            case JObjectKeyMin jObjectKeyMin -> {
                return 1;
            }
        }
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public byte[] bytes() {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer toByteBuffer() {
        var heapBb = StandardCharsets.UTF_8.encode(value);
        if (heapBb.isDirect()) return heapBb;
        var directBb = UninitializedByteBuffer.allocateUninitialized(heapBb.remaining());
        directBb.put(heapBb);
        directBb.flip();
        return directBb;
    }
}
