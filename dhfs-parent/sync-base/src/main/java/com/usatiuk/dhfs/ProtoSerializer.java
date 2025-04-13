package com.usatiuk.dhfs;

import com.google.protobuf.Message;

public interface ProtoSerializer<M extends Message, O> {
    O deserialize(M message);

    M serialize(O object);
}
