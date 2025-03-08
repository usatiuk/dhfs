package com.usatiuk.dhfs.objects.repository.peersync.api;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

@Path("/peer-info")
public interface PeerSyncApiClient {
    @Path("self")
    @GET
    ApiPeerInfo getSelfInfo();
}
