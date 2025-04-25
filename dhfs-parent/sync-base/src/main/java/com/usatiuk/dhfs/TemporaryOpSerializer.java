package com.usatiuk.dhfs;

import com.usatiuk.dhfs.invalidation.Op;
import com.usatiuk.dhfs.repository.OpP;
import com.usatiuk.utils.SerializationHelper;
import jakarta.inject.Singleton;

@Singleton
public class TemporaryOpSerializer implements ProtoSerializer<OpP, Op> {
    @Override
    public Op deserialize(OpP message) {
        return SerializationHelper.deserialize(message.getSerializedData().toByteArray());
    }

    @Override
    public OpP serialize(Op object) {
        return OpP.newBuilder()
                .setSerializedData(SerializationHelper.serialize(object))
                .build();
    }
}
