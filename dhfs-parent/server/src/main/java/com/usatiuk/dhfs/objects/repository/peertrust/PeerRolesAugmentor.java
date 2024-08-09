package com.usatiuk.dhfs.objects.repository.peertrust;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import io.quarkus.logging.Log;
import io.quarkus.security.credential.CertificateCredential;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;
import java.util.function.Supplier;

@ApplicationScoped
public class PeerRolesAugmentor implements SecurityIdentityAugmentor {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Override
    public Uni<SecurityIdentity> augment(SecurityIdentity identity, AuthenticationRequestContext context) {
        return Uni.createFrom().item(build(identity));
    }

    private Supplier<SecurityIdentity> build(SecurityIdentity identity) {
        if (identity.isAnonymous()) {
            return () -> identity;
        } else {
            QuarkusSecurityIdentity.Builder builder = QuarkusSecurityIdentity.builder(identity);

            var uuid = identity.getPrincipal().getName().substring(3);

            try {
                var entry = persistentPeerDataService.getHost(UUID.fromString(uuid));

                if (!entry.getCertificate().equals(identity.getCredential(CertificateCredential.class).getCertificate())) {
                    Log.error("Certificate mismatch for " + uuid);
                    return () -> identity;
                }

                builder.addRole("cluster-member");
                return builder::build;
            } catch (Exception e) {
                Log.error("Error when checking certificate for " + uuid, e);
                return () -> identity;
            }
        }
    }
}
