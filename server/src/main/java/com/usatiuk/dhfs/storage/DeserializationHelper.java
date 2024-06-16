package com.usatiuk.dhfs.storage;

import com.usatiuk.dhfs.storage.files.objects.File;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public abstract class DeserializationHelper {

    // Taken from SerializationUtils
    public static <T> T deserialize(final InputStream inputStream) {
        // Shitty hack to make it work with quarkus class loader
        var shit = new File(new UUID(0, 0)).getClass().getClassLoader();

        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(shit, inputStream)) {
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
