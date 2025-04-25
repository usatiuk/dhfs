package com.usatiuk.dhfs.peersync.api;

import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import java.security.cert.CertificateEncodingException;
import java.util.Base64;

@Path("/peer-info")
public class PeerSyncApi {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Path("self")
    @GET
    public ApiPeerInfo getSelfInfo() {
        try {
            return new ApiPeerInfo(persistentPeerDataService.getSelfUuid().toString(),
                    Base64.getEncoder().encodeToString(persistentPeerDataService.getSelfCertificate().getEncoded()));
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
