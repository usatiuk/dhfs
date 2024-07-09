package com.usatiuk.dhfs.objects.repository.peertrust;

import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import io.quarkus.security.credential.CertificateCredential;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.function.Supplier;

@ApplicationScoped
public class PeerRolesAugmentor implements SecurityIdentityAugmentor {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Override
    public Uni<SecurityIdentity> augment(SecurityIdentity identity, AuthenticationRequestContext context) {
        return Uni.createFrom().item(build(identity));
    }

    private Supplier<SecurityIdentity> build(SecurityIdentity identity) {
        if (identity.isAnonymous()) {
            return () -> identity;
        } else {
            QuarkusSecurityIdentity.Builder builder = QuarkusSecurityIdentity.builder(identity);

            // FIXME: The below is just an additional security check, we still check the certificates
            // with the normal TLS mechanisms.
            // But my guess is there's a race condition between tls store update and quarkus checking this somehow?
            // So the anonymous identity gets cached for a channel and it returns UNAUTHORIZED all the time...
            if (identity.getCredential(CertificateCredential.class).getCertificate() != null) {
                builder.addRole("cluster-member");
                return builder::build;
            }
            return () -> identity;
//            var uuid = identity.getPrincipal().getName().substring(3);
//
//            try {
//                var entry = persistentRemoteHostsService.getHost(UUID.fromString(uuid));
//
//                if (!entry.getCertificate().equals(identity.getCredential(CertificateCredential.class).getCertificate())) {
//                    Log.error("Certificate mismatch for " + uuid);
//                    return () -> identity;
//                }
//
//                builder.addRole("cluster-member");
//                return builder::build;
//            } catch (Exception e) {
//                Log.error("Error when checking certificate for " + uuid, e);
//                return () -> identity;
//            }
        }
    }
}
