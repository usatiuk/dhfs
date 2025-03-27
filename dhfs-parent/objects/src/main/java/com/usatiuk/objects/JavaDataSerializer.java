package com.usatiuk.objects;


import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.utils.SerializationHelper;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;

@ApplicationScoped
@DefaultBean
public class JavaDataSerializer implements ObjectSerializer<JData> {
    @Override
    public ByteString serialize(JData obj) {
        return SerializationHelper.serialize(obj);
    }

    @Override
    public JData deserialize(ByteString data) {
        try (var is = data.newInput()) {
            return SerializationHelper.deserialize(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
