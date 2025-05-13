package com.usatiuk.objects;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

/**
 * Interface for serializing and deserializing objects.
 *
 * @param <T> the type of object to serialize/deserialize
 */
public interface ObjectSerializer<T> {
    /**
     * Serialize an object to a ByteString.
     *
     * @param obj the object to serialize
     * @return the serialized object as a ByteString
     */
    ByteString serialize(T obj);

    /**
     * Deserialize an object from a ByteBuffer.
     *
     * @param data the ByteBuffer containing the serialized object
     * @return the deserialized object
     */
    T deserialize(ByteBuffer data);
}
