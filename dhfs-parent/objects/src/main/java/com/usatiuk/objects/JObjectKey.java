package com.usatiuk.objects;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * JObjectKey is an interface for object keys to be used in the object store.
 */
public sealed interface JObjectKey extends Serializable, Comparable<JObjectKey> permits JObjectKeyImpl, JObjectKeyMax, JObjectKeyMin {
    JObjectKeyMin MIN = new JObjectKeyMin();
    JObjectKeyMax MAX = new JObjectKeyMax();

    /**
     * Creates a new JObjectKey from a string value.
     *
     * @param value the string value of the key
     * @return a new JObjectKey
     */
    static JObjectKey of(String value) {
        return new JObjectKeyImpl(value);
    }

    /**
     * Creates a new JObjectKey with a random UUID.
     *
     * @return a new JObjectKey with a random UUID
     */
    static JObjectKey random() {
        return new JObjectKeyImpl(UUID.randomUUID().toString());
    }

    /**
     * Returns a JObjectKey that compares less than all other keys.
     * Calling value on this key will result in an exception.
     *
     * @return a JObjectKey that compares less than all other keys
     */
    static JObjectKey first() {
        return MIN;
    }

    /**
     * Returns a JObjectKey that compares greater than all other keys.
     * Calling value on this key will result in an exception.
     *
     * @return a JObjectKey that compares greater than all other keys
     */
    static JObjectKey last() {
        return MAX;
    }

    /**
     * Creates a new JObjectKey from a byte array.
     *
     * @param bytes the byte array representing the key
     * @return a new JObjectKey
     */
    static JObjectKey fromBytes(byte[] bytes) {
        return new JObjectKeyImpl(new String(bytes, StandardCharsets.ISO_8859_1));
    }

    /**
     * Creates a new JObjectKey from a ByteBuffer.
     *
     * @param buff the ByteBuffer representing the key
     * @return a new JObjectKey
     */
    static JObjectKey fromByteBuffer(ByteBuffer buff) {
        byte[] bytes = new byte[buff.remaining()];
        buff.get(bytes);
        return new JObjectKeyImpl(bytes);
    }

    @Override
    int compareTo(JObjectKey o);

    @Override
    String toString();

    /**
     * Returns the byte buffer representation of the key.
     *
     * @return the byte buffer representation of the key
     */
    ByteBuffer toByteBuffer();

    /**
     * Returns the string value of the key.
     *
     * @return the string value of the key
     */
    String value();
}
