package com.usatiuk.dhfs.repository.webapi;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.PeerManager;
import com.usatiuk.dhfs.repository.peersync.PeerInfoService;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import java.util.Collection;
import java.util.List;

@Path("/objects-manage")
public class ManagementApi {
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    PeerManager peerManager;

    @Path("known-peers")
    @GET
    public List<KnownPeerInfo> knownPeers() {
        return peerInfoService.getPeers().stream().map(peerInfo -> new KnownPeerInfo(peerInfo.id().toString())).toList();
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
    public Collection<AvailablePeerInfo> availablePeers() {
        return peerManager.getSeenButNotAddedHosts();
    }
}
