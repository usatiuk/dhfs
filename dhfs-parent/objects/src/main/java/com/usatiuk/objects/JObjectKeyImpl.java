package com.usatiuk.objects;

import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record JObjectKeyImpl(String name) implements JObjectKey {
    @Override
    public int compareTo(JObjectKey o) {
        switch (o) {
            case JObjectKeyImpl jObjectKeyImpl -> {
                return name.compareTo(jObjectKeyImpl.name());
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
        return name;
    }

    @Override
    public byte[] bytes() {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer toByteBuffer() {
        var heapBb = StandardCharsets.UTF_8.encode(name);
        if (heapBb.isDirect()) return heapBb;
        var directBb = UninitializedByteBuffer.allocateUninitialized(heapBb.remaining());
        directBb.put(heapBb);
        directBb.flip();
        return directBb;
    }
}
