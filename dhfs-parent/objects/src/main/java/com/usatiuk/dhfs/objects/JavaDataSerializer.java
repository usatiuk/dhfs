package com.usatiuk.dhfs.objects;


import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.Serializable;

@ApplicationScoped
public class JavaDataSerializer implements ObjectSerializer<JDataVersionedWrapper> {
    @Override
    public ByteString serialize(JDataVersionedWrapper obj) {
        return SerializationHelper.serialize((Serializable) obj);
    }

    @Override
    public JDataVersionedWrapper deserialize(ByteString data) {
        return SerializationHelper.deserialize(data.toByteArray());
    }
}
