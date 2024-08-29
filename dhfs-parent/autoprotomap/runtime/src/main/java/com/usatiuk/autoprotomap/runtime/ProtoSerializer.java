package com.usatiuk.autoprotomap.runtime;

public interface ProtoSerializer<M, O> {
//    O deserialize(M message);

    M serialize(O object);
}
