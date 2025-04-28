package com.usatiuk.objects;


import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;

import java.nio.ByteBuffer;

@ApplicationScoped
@DefaultBean
public class JavaDataSerializer implements ObjectSerializer<JData> {
    private static final ThreadSafeFury fury = Fury.builder().withLanguage(Language.JAVA)
            // Allow to deserialize objects unknown types,
            // more flexible but less secure.
            .requireClassRegistration(false)
            .buildThreadSafeFury();

    @Override
    public ByteString serialize(JData obj) {
        return UnsafeByteOperations.unsafeWrap(fury.serialize(obj));
    }

    @Override
    public JData deserialize(ByteBuffer data) {
        return (JData) fury.deserialize(data);
    }
}
