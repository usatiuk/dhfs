package com.usatiuk.dhfs.storage;

import com.usatiuk.dhfs.storage.files.objects.File;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public abstract class DeserializationHelper {

    // Taken from SerializationUtils
    public static <T> T deserialize(final InputStream inputStream) {
        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(File.class.getClassLoader(), inputStream)) {
            final T obj = (T) in.readObject();
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(final byte[] objectData) {
        return deserialize(new ByteArrayInputStream(objectData));
    }
}
