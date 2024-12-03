package com.usatiuk.dhfs.objects;


import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.utils.SerializationHelper;
import com.usatiuk.objects.common.runtime.JData;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.Serializable;

@ApplicationScoped
public class JavaDataSerializer implements ObjectSerializer<JData> {
    @Override
    public ByteString serialize(JData obj) {
        return SerializationHelper.serialize((Serializable) obj);
    }

    @Override
    public JData deserialize(ByteString data) {
        return SerializationHelper.deserialize(data.toByteArray());
    }
}
