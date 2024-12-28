package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.kleppmanntree.PeerInterface;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Singleton
public class JKleppmannTreePeerInterface implements PeerInterface<UUID> {
    @Override
    public UUID getSelfId() {
        return UUID.nameUUIDFromBytes("1".getBytes());
    }

    @Override
    public Collection<UUID> getAllPeers() {
        return List.of(getSelfId());
    }
}
