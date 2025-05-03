package com.usatiuk.dhfs.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.peertrust.CertificateTools;
import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.dhfs.remoteobj.JDataRemotePush;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import java.security.cert.X509Certificate;

@JDataRemotePush
public record PeerInfo(JObjectKey key, PeerId id, ByteString cert,
                       PMap<PeerId, Long> kickCounter,
                       long lastSeenTimestamp) implements JDataRemote, JDataRemoteDto {
    public PeerInfo(PeerId id, byte[] cert) {
        this(id.toJObjectKey(), id, ByteString.copyFrom(cert), HashTreePMap.empty(), System.currentTimeMillis());
    }

    public X509Certificate parsedCert() {
        return CertificateTools.certFromBytes(cert.toByteArray());
    }

    public PeerInfo withKickCounter(PMap<PeerId, Long> kickCounter) {
        return new PeerInfo(key, id, cert, kickCounter, lastSeenTimestamp);
    }

    public PeerInfo withIncrementedKickCounter(PeerId peerId) {
        return new PeerInfo(key, id, cert, kickCounter.plus(peerId, kickCounter.getOrDefault(peerId, 0L) + 1), lastSeenTimestamp);
    }

    public PeerInfo withLastSeenTimestamp(long lastSeenTimestamp) {
        return new PeerInfo(key, id, cert, kickCounter, lastSeenTimestamp);
    }

    public long kickCounterSum() {
        return kickCounter.values().stream().mapToLong(Long::longValue).sum();
    }

    @Override
    public String toString() {
        return "PeerInfo{" +
                "key=" + key +
                ", id=" + id +
                ", kickCounter=" + kickCounter +
                ", lastSeenTimestamp=" + lastSeenTimestamp +
                '}';
    }
}
