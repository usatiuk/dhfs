package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.PeerDirectoryLocalP;
import jakarta.inject.Singleton;

import java.util.Objects;
import java.util.UUID;

@Singleton
public class PeerDirectoryLocalSerializer implements ProtoSerializer<PeerDirectoryLocalP, PeerDirectoryLocal> {
    @Override
    public PeerDirectoryLocal deserialize(PeerDirectoryLocalP message) {
        var ret = new PeerDirectoryLocal();
        ret.getInitialOpSyncDone().addAll(message.getInitialOpSyncDonePeersList().stream().map(UUID::fromString).toList());
        ret.getInitialObjSyncDone().addAll(message.getInitialObjSyncDonePeersList().stream().map(UUID::fromString).toList());
        return ret;
    }

    @Override
    public PeerDirectoryLocalP serialize(PeerDirectoryLocal object) {
        return PeerDirectoryLocalP.newBuilder()
                .addAllInitialObjSyncDonePeers(() -> object.getInitialObjSyncDone().stream().map(Objects::toString).iterator())
                .addAllInitialOpSyncDonePeers(() -> object.getInitialOpSyncDone().stream().map(Objects::toString).iterator()).build();
    }
}
