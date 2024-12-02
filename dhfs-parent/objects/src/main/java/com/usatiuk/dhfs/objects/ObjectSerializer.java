package com.usatiuk.dhfs.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.common.JData;

public interface ObjectSerializer<T extends JData> {
    ByteString serialize(T obj);

    T deserialize(ByteString data);
}
