package com.usatiuk.dhfs.webapi;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PeerManager;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

@Path("/peers-manage")
public class PeerManagementApi {
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    PeerManager peerManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Path("known-peers")
    @GET
    public KnownPeers knownPeers() {
        return new KnownPeers(peerInfoService.getPeers().stream().map(peerInfo -> new KnownPeerInfo(peerInfo.id().toString(),
                Optional.ofNullable(peerManager.getAddress(peerInfo.id())).map(Objects::toString).orElse(null))).toList(),
                persistentPeerDataService.getSelfUuid().toString());
    }

    @Path("known-peers")
    @PUT
    public void addPeer(KnownPeerPut knownPeerPut) {
        peerManager.addRemoteHost(PeerId.of(knownPeerPut.uuid()));
    }

    @Path("known-peers")
    @DELETE
    public void deletePeer(KnownPeerDelete knownPeerDelete) {
        peerManager.removeRemoteHost(PeerId.of(knownPeerDelete.uuid()));
    }

    @Path("available-peers")
    @GET
    public Collection<KnownPeerInfo> availablePeers() {
        return peerManager.getSeenButNotAddedHosts().stream()
                .map(p -> new KnownPeerInfo(p.toString(),
                        peerManager.selectBestAddress(p).map(Objects::toString).orElse(null)))
                .toList();
    }

    @Path("peer-state")
    @GET
    public Collection<PeerInfo> peerInfos(Collection<String> peerIdStrings) {
        return peerIdStrings.stream().map(PeerId::of).map(
                peerId -> {
                    return new PeerInfo(
                            peerId.toString(),
                            peerManager.getAddress(peerId).toString()
                    );
                }
        ).toList();
    }
}
