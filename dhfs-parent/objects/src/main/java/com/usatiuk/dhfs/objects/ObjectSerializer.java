package com.usatiuk.dhfs.objects;

import com.google.protobuf.ByteString;

public interface ObjectSerializer<T> {
    ByteString serialize(T obj);

    T deserialize(ByteString data);
}
