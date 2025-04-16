package com.usatiuk.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public sealed interface JObjectKey extends Serializable, Comparable<JObjectKey> permits JObjectKeyImpl, JObjectKeyMax, JObjectKeyMin {
    JObjectKeyMin MIN = new JObjectKeyMin();
    JObjectKeyMax MAX = new JObjectKeyMax();

    static JObjectKey of(String value) {
        var heapBb = StandardCharsets.UTF_8.encode(value);
        if (heapBb.isDirect()) return new JObjectKeyImpl(heapBb);
        return fromByteBuffer(heapBb);
    }

    static JObjectKey of(ByteString value) {
        var heapBb = value.asReadOnlyByteBuffer();
        if (heapBb.isDirect()) return new JObjectKeyImpl(heapBb);
        return fromByteBuffer(heapBb);
    }

    static JObjectKey random() {
        return JObjectKey.of(UUID.randomUUID().toString());
    }

    static JObjectKey first() {
        return MIN;
    }

    static JObjectKey last() {
        return MAX;
    }

    static JObjectKey fromByteBuffer(ByteBuffer buff) {
        var directBb = UninitializedByteBuffer.allocateUninitialized(buff.remaining());
        directBb.put(buff);
        directBb.flip();
        return new JObjectKeyImpl(directBb);
    }

    @Override
    int compareTo(JObjectKey o);

    @Override
    String toString();

    ByteBuffer toByteBuffer();

    ByteString value();
}
