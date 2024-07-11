package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import java.security.cert.CertificateEncodingException;
import java.util.Base64;

@Path("/peer-info")
public class PeerSyncApi {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Path("self")
    @GET
    public PeerInfo getSelfInfo() {
        try {
            return new PeerInfo(persistentRemoteHostsService.getSelfUuid().toString(),
                    Base64.getEncoder().encodeToString(persistentRemoteHostsService.getSelfCertificate().getEncoded()));
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
