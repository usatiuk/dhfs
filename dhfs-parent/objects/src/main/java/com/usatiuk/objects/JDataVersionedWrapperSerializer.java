package com.usatiuk.objects;


import com.google.protobuf.ByteString;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.ByteBuffer;

@Singleton
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
        return new JDataVersionedWrapperLazy(version, rawData.size(),
                () -> dataSerializer.deserialize(rawData)
        );
    }
}
