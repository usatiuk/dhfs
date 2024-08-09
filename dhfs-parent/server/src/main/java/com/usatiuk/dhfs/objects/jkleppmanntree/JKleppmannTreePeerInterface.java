package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.kleppmanntree.PeerInterface;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.UUID;

@Singleton
public class JKleppmannTreePeerInterface implements PeerInterface<UUID> {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Override
    public UUID getSelfId() {
        return persistentPeerDataService.getSelfUuid();
    }

    @Override
    public Collection<UUID> getAllPeers() {
        return persistentPeerDataService.getHostUuidsAndSelf();
    }
}
