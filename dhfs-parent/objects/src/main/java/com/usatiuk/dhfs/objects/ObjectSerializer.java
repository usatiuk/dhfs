package com.usatiuk.dhfs.objects;

import com.google.protobuf.ByteString;

public interface ObjectSerializer<T extends JData> {
    ByteString serialize(T obj);

    T deserialize(ByteString data);
}
