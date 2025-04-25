package com.usatiuk.dhfs.peersync;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.persistence.PeerInfoP;
import com.usatiuk.utils.SerializationHelper;
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
