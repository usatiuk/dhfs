package com.usatiuk.dhfs.objects.serializer;


import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.ObjectSerializer;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.Serializable;

@ApplicationScoped
public class TestJDataSerializer implements ObjectSerializer<JData> {
    @Override
    public ByteString serialize(JData obj) {
        return SerializationHelper.serialize((Serializable) obj);
    }

    @Override
    public JData deserialize(ByteString data) {
        return SerializationHelper.deserialize(data.toByteArray());
    }
}
