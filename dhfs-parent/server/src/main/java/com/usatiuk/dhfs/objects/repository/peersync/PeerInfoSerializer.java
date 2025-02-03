package com.usatiuk.dhfs.objects.repository.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.persistence.PeerInfoP;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PeerInfoSerializer implements ProtoSerializer<PeerInfoP, PeerInfo> {

    @Override
    public PeerInfo deserialize(PeerInfoP message) {
        return new PeerInfo(PeerId.of(message.getUuid()), message.getCert().toByteArray());
    }

    @Override
    public PeerInfoP serialize(PeerInfo object) {
        return PeerInfoP.newBuilder()
                .setUuid(object.key().toString())
                .setCert(ByteString.copyFrom(object.cert()))
                .build();
    }
}
