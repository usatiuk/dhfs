package com.usatiuk.dhfs.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.JDataRemoteP;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.inject.Singleton;

@Singleton
public class TemporaryJDataRemoteSerializer implements ProtoSerializer<JDataRemoteP, JDataRemote> {
    @Override
    public JDataRemote deserialize(JDataRemoteP message) {
        return SerializationHelper.deserialize(message.getSerializedData().toByteArray());
    }

    @Override
    public JDataRemoteP serialize(JDataRemote object) {
        return JDataRemoteP.newBuilder()
                .setSerializedData(SerializationHelper.serialize(object))
                .build();
    }
}
