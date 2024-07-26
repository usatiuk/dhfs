package com.usatiuk.dhfs.objects.protoserializer;

import com.google.protobuf.Message;

public interface ProtoDeserializer<M extends Message, O> {
    O deserialize(M message);
}
