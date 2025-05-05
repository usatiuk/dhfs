package com.usatiuk.dhfs.webapi;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.dhfs.peersync.ConnectedPeerManager;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;

import java.util.*;

@Path("/peers-manage")
public class PeerManagementApi {
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    ConnectedPeerManager connectedPeerManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Path("known-peers")
    @GET
    public List<PeerInfo> knownPeers() {
        return peerInfoService.getPeers().stream().map(
                peerInfo -> new PeerInfo(peerInfo.id().toString(), Base64.getEncoder().encodeToString(peerInfo.cert().toByteArray()),
                        Optional.ofNullable(connectedPeerManager.getAddress(peerInfo.id())).map(Objects::toString).orElse(null))).toList();
    }

    @Path("known-peers/{peerId}")
    @PUT
    public void addPeer(@PathParam("peerId") String peerId, KnownPeerPut knownPeerPut) {
        connectedPeerManager.addRemoteHost(PeerId.of(peerId), knownPeerPut.cert());
    }

    @Path("known-peers/{peerId}")
    @DELETE
    public void deletePeer(@PathParam("peerId") String peerId) {
        connectedPeerManager.removeRemoteHost(PeerId.of(peerId));
    }

    @Path("available-peers")
    @GET
    public Collection<PeerInfo> availablePeers() {
        return connectedPeerManager.getSeenButNotAddedHosts().stream()
                .map(p -> new PeerInfo(p.getLeft().toString(), p.getRight().cert(),
                        connectedPeerManager.selectBestAddress(p.getLeft()).map(Objects::toString).orElse(null)))
                .toList();
    }
}
