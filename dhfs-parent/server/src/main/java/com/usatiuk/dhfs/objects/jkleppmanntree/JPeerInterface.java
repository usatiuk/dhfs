package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.kleppmanntree.PeerInterface;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.UUID;

@Singleton
public class JPeerInterface implements PeerInterface<UUID> {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Override
    public UUID getSelfId() {
        return persistentRemoteHostsService.getSelfUuid();
    }

    @Override
    public Collection<UUID> getAllPeers() {
        return persistentRemoteHostsService.getHostUuids();
    }
}
