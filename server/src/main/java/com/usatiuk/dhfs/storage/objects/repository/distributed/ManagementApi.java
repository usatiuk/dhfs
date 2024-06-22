package com.usatiuk.dhfs.storage.objects.repository.distributed;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import java.util.List;

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
    public void addPeer(HostInfo hostInfo) {
        persistentRemoteHostsService.addHost(hostInfo);
    }
}
