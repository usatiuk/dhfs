package com.usatiuk.objects;


import com.google.protobuf.ByteString;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.ByteBuffer;

/**
 * Serializer for JDataVersionedWrapper objects.
 * The objects are stored in a simple format: first is 8-byte long, then the serialized object.
 */
@Singleton
public class JDataVersionedWrapperSerializer {
    @Inject
    ObjectSerializer<JData> dataSerializer;

    /**
     * Serializes a JDataVersionedWrapper object to a ByteString.
     *
     * @param obj the object to serialize
     * @return the serialized object as a ByteString
     */
    public ByteString serialize(JDataVersionedWrapper obj) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(obj.version());
        buffer.flip();
        return ByteString.copyFrom(buffer).concat(dataSerializer.serialize(obj.data()));
    }

    /**
     * Deserializes a JDataVersionedWrapper object from a ByteBuffer.
     * Returns a lazy wrapper (JDataVersionedWrapperLazy).
     *
     * @param data the ByteBuffer containing the serialized object
     * @return the deserialized object
     */
    public JDataVersionedWrapper deserialize(ByteBuffer data) {
        var version = data.getLong();
        return new JDataVersionedWrapperLazy(version, data.remaining(),
                () -> dataSerializer.deserialize(data)
        );
    }
}
