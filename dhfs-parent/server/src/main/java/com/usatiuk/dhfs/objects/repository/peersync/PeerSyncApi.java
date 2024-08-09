package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
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
    public PeerInfo getSelfInfo() {
        try {
            return new PeerInfo(persistentPeerDataService.getSelfUuid().toString(),
                                Base64.getEncoder().encodeToString(persistentPeerDataService.getSelfCertificate().getEncoded()));
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
