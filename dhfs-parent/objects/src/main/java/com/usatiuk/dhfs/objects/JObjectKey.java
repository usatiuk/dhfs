package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public record JObjectKey(String name) implements Serializable, Comparable<JObjectKey> {
    public static JObjectKey of(String name) {
        return new JObjectKey(name);
    }

    public static JObjectKey random() {
        return new JObjectKey(UUID.randomUUID().toString());
    }

    public static JObjectKey first() {
        return new JObjectKey("");
    }

    @Override
    public int compareTo(JObjectKey o) {
        return name.compareTo(o.name);
    }

    @Override
    public String toString() {
        return name;
    }

    public byte[] bytes() {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    public ByteBuffer toByteBuffer() {
        var heapBb = StandardCharsets.UTF_8.encode(name);
        if (heapBb.isDirect()) return heapBb;
        var directBb = UninitializedByteBuffer.allocateUninitialized(heapBb.remaining());
        directBb.put(heapBb);
        directBb.flip();
        return directBb;
    }

    public static JObjectKey fromBytes(byte[] bytes) {
        return new JObjectKey(new String(bytes, StandardCharsets.UTF_8));
    }

    public static JObjectKey fromByteBuffer(ByteBuffer buff) {
        return new JObjectKey(StandardCharsets.UTF_8.decode(buff).toString());
    }
}
