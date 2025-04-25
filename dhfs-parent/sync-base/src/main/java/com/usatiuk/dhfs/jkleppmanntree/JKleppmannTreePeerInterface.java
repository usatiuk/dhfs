package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.peersync.PeerInfo;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.kleppmanntree.PeerInterface;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;

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
