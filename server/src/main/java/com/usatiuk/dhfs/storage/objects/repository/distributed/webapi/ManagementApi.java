package com.usatiuk.dhfs.storage.objects.repository.distributed.webapi;

import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteHostManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Path("/objects-manage")
public class ManagementApi {
    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Path("known-peers")
    @GET
    public List<KnownPeerInfo> knownPeers() {
        return persistentRemoteHostsService.getHosts().stream().map(h -> new KnownPeerInfo(h.getUuid().toString())).toList();
    }

    @Path("known-peers")
    @PUT
    public void addPeer(KnownPeerPut knownPeerPut) {
        remoteHostManager.addRemoteHost(UUID.fromString(knownPeerPut.uuid()));
    }

    @Path("available-peers")
    @GET
    public Collection<AvailablePeerInfo> availablePeers() {
        return remoteHostManager.getSeenButNotAddedHosts();
    }
}
