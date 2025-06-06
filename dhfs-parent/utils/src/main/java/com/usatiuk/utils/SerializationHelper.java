package com.usatiuk.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang3.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Utility class for serialization and deserialization of objects.
 * <p>
 * This class provides methods to serialize and deserialize objects using Java's built-in serialization mechanism.
 * It also includes methods to handle byte arrays and input streams for serialization and deserialization.
 * </p>
 */
public abstract class SerializationHelper {
    // Taken from SerializationUtils
    public static <T> T deserialize(final InputStream inputStream) {
        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(SerializationHelper.class.getClassLoader(), inputStream)) {
            final T obj = (T) in.readObject();
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(final byte[] objectData, int offset) {
        return deserialize(new ByteArrayInputStream(objectData, offset, objectData.length - offset));
    }

    public static <T> T deserialize(final byte[] objectData) {
        return deserialize(objectData, 0);
    }

    public static <T extends Serializable> byte[] serializeArray(final T obj) {
        return SerializationUtils.serialize(obj);
    }

    public static <T extends Serializable> ByteString serialize(final T obj) {
        return UnsafeByteOperations.unsafeWrap(serializeArray(obj));
    }
}
