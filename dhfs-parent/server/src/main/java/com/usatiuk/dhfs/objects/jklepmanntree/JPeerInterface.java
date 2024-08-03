package com.usatiuk.dhfs.objects.jklepmanntree;

import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.kleppmanntree.PeerInterface;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.NotImplementedException;

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
        throw new NotImplementedException();
    }
}
