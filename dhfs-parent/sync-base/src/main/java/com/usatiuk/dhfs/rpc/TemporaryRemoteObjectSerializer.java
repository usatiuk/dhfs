package com.usatiuk.dhfs.rpc;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.persistence.JDataRemoteDtoP;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.utils.SerializationHelper;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TemporaryRemoteObjectSerializer implements ProtoSerializer<JDataRemoteDtoP, JDataRemoteDto> {
    @Override
    public JDataRemoteDto deserialize(JDataRemoteDtoP message) {
        return SerializationHelper.deserialize(message.getSerializedData().toByteArray());
    }

    @Override
    public JDataRemoteDtoP serialize(JDataRemoteDto object) {
        return JDataRemoteDtoP.newBuilder()
                .setSerializedData(SerializationHelper.serialize(object))
                .build();
    }
}
