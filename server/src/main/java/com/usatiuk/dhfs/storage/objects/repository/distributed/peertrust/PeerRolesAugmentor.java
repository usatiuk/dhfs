package com.usatiuk.dhfs.storage.objects.repository.distributed.peertrust;

import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
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

            var entry = persistentRemoteHostsService.getHosts().stream()
                    .filter(i -> i.getUuid().toString()
                            .equals(identity.getPrincipal().getName().substring(3)))
                    .findFirst();
            if (entry.isEmpty()) return () -> identity;

            if (!entry.get().getCertificate().equals(identity.getCredential(CertificateCredential.class).getCertificate()))
                return () -> identity;

            builder.addRole("cluster-member");
            return builder::build;
        }
    }
}
