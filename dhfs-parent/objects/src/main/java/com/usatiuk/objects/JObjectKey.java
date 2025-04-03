package com.usatiuk.objects;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public sealed interface JObjectKey extends Serializable, Comparable<JObjectKey> permits JObjectKeyImpl, JObjectKeyMax, JObjectKeyMin {
    JObjectKeyMin MIN = new JObjectKeyMin();
    JObjectKeyMax MAX = new JObjectKeyMax();

    static JObjectKey of(String name) {
        return new JObjectKeyImpl(name);
    }

    static JObjectKey random() {
        return new JObjectKeyImpl(UUID.randomUUID().toString());
    }

    static JObjectKey first() {
        return MIN;
    }

    static JObjectKey last() {
        return MAX;
    }

    static JObjectKey fromBytes(byte[] bytes) {
        return new JObjectKeyImpl(new String(bytes, StandardCharsets.UTF_8));
    }

    static JObjectKey fromByteBuffer(ByteBuffer buff) {
        return new JObjectKeyImpl(StandardCharsets.UTF_8.decode(buff).toString());
    }

    @Override
    int compareTo(JObjectKey o);

    @Override
    String toString();

    byte[] bytes();

    ByteBuffer toByteBuffer();

    String name();
}
