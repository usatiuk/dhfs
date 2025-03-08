package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfoService;
import com.usatiuk.kleppmanntree.PeerInterface;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Singleton
public class JKleppmannTreePeerInterface implements PeerInterface<PeerId> {
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Override
    public PeerId getSelfId() {
        return persistentPeerDataService.getSelfUuid();
    }

    @Override
    public Collection<PeerId> getAllPeers() {
        return peerInfoService.getPeers().stream().map(PeerInfo::id).toList();
    }
}
