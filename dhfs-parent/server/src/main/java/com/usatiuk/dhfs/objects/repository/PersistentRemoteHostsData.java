package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

public record PersistentRemoteHostsData(PeerId selfUuid,
                                        long selfCounter,
                                        X509Certificate selfCertificate,
                                        KeyPair selfKeyPair) implements JData, Serializable {
    public static final JObjectKey KEY = JObjectKey.of("self_peer_data");

    @Override
    public JObjectKey key() {
        return KEY;
    }

    public PersistentRemoteHostsData withSelfCounter(long selfCounter) {
        return new PersistentRemoteHostsData(selfUuid, selfCounter, selfCertificate, selfKeyPair);
    }
}
