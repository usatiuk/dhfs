package com.usatiuk.objects;


import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.utils.SerializationHelper;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple Java object serializer.
 */
@ApplicationScoped
@DefaultBean
public class JavaDataSerializer implements ObjectSerializer<JData> {
    @Override
    public ByteString serialize(JData obj) {
        return SerializationHelper.serialize(obj);
    }

    public JData deserialize(ByteBuffer data) {
        try (var is = UnsafeByteOperations.unsafeWrap(data).newInput()) {
            return SerializationHelper.deserialize(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
