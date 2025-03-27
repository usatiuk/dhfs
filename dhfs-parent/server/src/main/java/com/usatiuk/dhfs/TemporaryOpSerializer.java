package com.usatiuk.dhfs;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.repository.OpP;
import com.usatiuk.dhfs.repository.invalidation.Op;
import com.usatiuk.dhfs.utils.SerializationHelper;
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
