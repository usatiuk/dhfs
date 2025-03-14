package com.usatiuk.dhfs.objects;


import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.ByteBuffer;

@ApplicationScoped
public class JDataVersionedWrapperSerializer implements ObjectSerializer<JDataVersionedWrapper> {
    @Inject
    ObjectSerializer<JData> dataSerializer;

    @Override
    public ByteString serialize(JDataVersionedWrapper obj) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(obj.version());
        buffer.flip();
        return ByteString.copyFrom(buffer).concat(dataSerializer.serialize(obj.data()));
    }

    @Override
    public JDataVersionedWrapper deserialize(ByteString data) {
        var version = data.substring(0, Long.BYTES).asReadOnlyByteBuffer().getLong();
        var rawData = data.substring(Long.BYTES);
        return new JDataVersionedWrapperLazy(version, rawData.size(), () -> dataSerializer.deserialize(rawData));
    }
}
