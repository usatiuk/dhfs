package com.usatiuk.objects;


import com.google.protobuf.ByteString;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.ByteBuffer;

@Singleton
public class JDataVersionedWrapperSerializer {
    @Inject
    ObjectSerializer<JData> dataSerializer;

    public ByteString serialize(JDataVersionedWrapper obj) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(obj.version());
        buffer.flip();
        return ByteString.copyFrom(buffer).concat(dataSerializer.serialize(obj.data()));
    }

    public JDataVersionedWrapper deserialize(ByteBuffer data) {
        var version = data.getLong();
        return new JDataVersionedWrapperLazy(version, data.remaining(),
                () -> dataSerializer.deserialize(data)
        );
    }
}
