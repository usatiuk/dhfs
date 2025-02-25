package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import org.pcollections.PSet;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

public record PersistentRemoteHostsData(PeerId selfUuid,
                                        X509Certificate selfCertificate,
                                        KeyPair selfKeyPair,
                                        PSet<PeerId> initialSyncDone) implements JData, Serializable {
    public static final JObjectKey KEY = JObjectKey.of("self_peer_data");

    @Override
    public JObjectKey key() {
        return KEY;
    }


    public PersistentRemoteHostsData withInitialSyncDone(PSet<PeerId> initialSyncDone) {
        return new PersistentRemoteHostsData(selfUuid, selfCertificate, selfKeyPair, initialSyncDone);
    }

    @Override
    public String toString() {
        return "PersistentRemoteHostsData{" +
                "selfUuid=" + selfUuid +
                ", initialSyncDone=" + initialSyncDone +
                '}';
    }
}
