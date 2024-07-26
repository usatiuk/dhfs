package com.usatiuk.dhfs.objects.protoserializer;

import com.google.protobuf.Message;

public interface ProtoSerializer<M extends Message, O> {
    M serialize(O object);
}
