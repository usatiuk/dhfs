package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.peerdiscovery.IpPeerAddress;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

public record PersistentRemoteHostsData(PeerId selfUuid,
                                        X509Certificate selfCertificate,
                                        KeyPair selfKeyPair,
                                        PSet<PeerId> initialSyncDone,
                                        PMap<PeerId, IpPeerAddress> persistentPeerAddress) implements JData, Serializable {
    public static final JObjectKey KEY = JObjectKey.of("self_peer_data");

    @Override
    public JObjectKey key() {
        return KEY;
    }

    public PersistentRemoteHostsData withInitialSyncDone(PSet<PeerId> initialSyncDone) {
        return new PersistentRemoteHostsData(selfUuid, selfCertificate, selfKeyPair, initialSyncDone, persistentPeerAddress);
    }

    public PersistentRemoteHostsData withPersistentPeerAddress(PMap<PeerId, IpPeerAddress> persistentPeerAddress) {
        return new PersistentRemoteHostsData(selfUuid, selfCertificate, selfKeyPair, initialSyncDone, persistentPeerAddress);
    }

    @Override
    public String toString() {
        return "PersistentRemoteHostsData{" +
                "selfUuid=" + selfUuid +
                ", initialSyncDone=" + initialSyncDone +
                '}';
    }
}
