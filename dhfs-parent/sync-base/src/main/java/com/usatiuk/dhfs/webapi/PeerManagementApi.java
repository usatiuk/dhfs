package com.usatiuk.dhfs.webapi;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.dhfs.peersync.PeerManager;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;

import java.util.*;

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
    public List<PeerInfo> knownPeers() {
        return peerInfoService.getPeers().stream().map(
                peerInfo -> new PeerInfo(peerInfo.id().toString(), Base64.getEncoder().encodeToString(peerInfo.cert().toByteArray()),
                        Optional.ofNullable(peerManager.getAddress(peerInfo.id())).map(Objects::toString).orElse(null))).toList();
    }

    @Path("known-peers/{peerId}")
    @PUT
    public void addPeer(@PathParam("peerId") String peerId, KnownPeerPut knownPeerPut) {
        peerManager.addRemoteHost(PeerId.of(peerId), knownPeerPut.cert());
    }

    @Path("known-peers/{peerId}")
    @DELETE
    public void deletePeer(@PathParam("peerId") String peerId) {
        peerManager.removeRemoteHost(PeerId.of(peerId));
    }

    @Path("available-peers")
    @GET
    public Collection<PeerInfo> availablePeers() {
        return peerManager.getSeenButNotAddedHosts().stream()
                .map(p -> new PeerInfo(p.getLeft().toString(), p.getRight().cert(),
                        peerManager.selectBestAddress(p.getLeft()).map(Objects::toString).orElse(null)))
                .toList();
    }
}
