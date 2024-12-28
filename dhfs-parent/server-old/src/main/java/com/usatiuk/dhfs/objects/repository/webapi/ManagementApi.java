package com.usatiuk.dhfs.objects.repository.webapi;

import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Path("/objects-manage")
public class ManagementApi {
    @Inject
    PeerManager remoteHostManager;

    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Path("known-peers")
    @GET
    public List<KnownPeerInfo> knownPeers() {
        return persistentPeerDataService.getHostsNoNulls().stream().map(h -> new KnownPeerInfo(h.getUuid().toString())).toList();
    }

    @Path("known-peers")
    @PUT
    public void addPeer(KnownPeerPut knownPeerPut) {
        remoteHostManager.addRemoteHost(UUID.fromString(knownPeerPut.uuid()));
    }

    @Path("known-peers")
    @DELETE
    public void DeletePeer(KnownPeerDelete knownPeerDelete) {
        remoteHostManager.removeRemoteHost(UUID.fromString(knownPeerDelete.uuid()));
    }

    @Path("available-peers")
    @GET
    public Collection<AvailablePeerInfo> availablePeers() {
        return remoteHostManager.getSeenButNotAddedHosts();
    }
}
