package com.usatiuk.dhfs.repository.peersync;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.persistence.PeerInfoP;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.inject.Singleton;

import java.io.IOException;

@Singleton
public class PeerInfoProtoSerializer implements ProtoSerializer<PeerInfoP, PeerInfo> {
    @Override
    public PeerInfo deserialize(PeerInfoP message) {
        try (var is = message.getSerializedData().newInput()) {
            return SerializationHelper.deserialize(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PeerInfoP serialize(PeerInfo object) {
        return PeerInfoP.newBuilder().setSerializedData(SerializationHelper.serialize(object)).build();
    }
}
