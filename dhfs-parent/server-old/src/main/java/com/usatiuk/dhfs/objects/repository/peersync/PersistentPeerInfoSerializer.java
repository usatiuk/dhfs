package com.usatiuk.dhfs.objects.repository.peersync;

import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.PersistentPeerInfoP;
import com.usatiuk.dhfs.objects.repository.CertificateTools;
import jakarta.inject.Singleton;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.util.UUID;

@Singleton
public class PersistentPeerInfoSerializer implements ProtoSerializer<PersistentPeerInfoP, PersistentPeerInfo> {
    @Override
    public PersistentPeerInfo deserialize(PersistentPeerInfoP message) {
        try {
            return new PersistentPeerInfo(
                    UUID.fromString(message.getUuid()),
                    CertificateTools.certFromBytes(message.getCert().toByteArray())
            );
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PersistentPeerInfoP serialize(PersistentPeerInfo object) {
        try {
            return PersistentPeerInfoP.newBuilder()
                    .setUuid(object.getUuid().toString())
                    .setCert(UnsafeByteOperations.unsafeWrap(object.getCertificate().getEncoded()))
                    .build();
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
