package com.usatiuk.dhfs.storage.objects.repository.distributed.webapi;

import com.usatiuk.dhfs.storage.objects.repository.distributed.HostInfo;
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
    public List<HostInfo> knownPeers() {
        return persistentRemoteHostsService.getHosts();
    }

    @Path("known-peers")
    @PUT
    public void addPeer(String hostname) {
        remoteHostManager.addRemoteHost(UUID.fromString(hostname));
    }

    @Path("available-peers")
    @GET
    public Collection<AvailablePeerInfo> availablePeers() {
        return remoteHostManager.getSeenButNotAddedHosts();
    }
}
