package com.usatiuk.dhfs.objects;


import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.ByteBuffer;

@ApplicationScoped
public class JavaDataSerializer implements ObjectSerializer<JDataVersionedWrapper> {
    @Override
    public ByteString serialize(JDataVersionedWrapper obj) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(obj.version());
        buffer.flip();
        return ByteString.copyFrom(buffer).concat(SerializationHelper.serialize(obj.data()));
    }

    @Override
    public JDataVersionedWrapper deserialize(ByteString data) {
        var version = data.substring(0, Long.BYTES).asReadOnlyByteBuffer().getLong();
        var rawData = data.substring(Long.BYTES);
        return new JDataVersionedWrapperLazy(version, rawData);
    }
}
