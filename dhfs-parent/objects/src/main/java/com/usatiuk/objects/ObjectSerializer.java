package com.usatiuk.objects;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public interface ObjectSerializer<T> {
    ByteString serialize(T obj);

    T deserialize(ByteBuffer data);
}
