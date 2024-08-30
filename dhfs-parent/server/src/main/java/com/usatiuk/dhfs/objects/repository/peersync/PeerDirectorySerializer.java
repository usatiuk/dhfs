package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.PeerDirectoryP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;

import java.util.Objects;
import java.util.UUID;

@Singleton
public class PeerDirectorySerializer implements ProtoSerializer<PeerDirectoryP, PeerDirectory> {
    @Override
    public PeerDirectory deserialize(PeerDirectoryP message) {
        var ret = new PeerDirectory();
        message.getPeersList().stream().map(UUID::fromString).forEach(ret.getPeers()::add);
        return ret;
    }

    @Override
    public PeerDirectoryP serialize(PeerDirectory object) {
        return PeerDirectoryP.newBuilder().addAllPeers(() -> object.getPeers().stream().map(Objects::toString).iterator()).build();
    }
}
