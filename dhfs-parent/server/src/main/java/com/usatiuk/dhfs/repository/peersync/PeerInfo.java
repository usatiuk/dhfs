package com.usatiuk.dhfs.repository.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.CertificateTools;
import com.usatiuk.dhfs.repository.JDataRemoteDto;
import com.usatiuk.dhfs.repository.JDataRemotePush;

import java.security.cert.X509Certificate;

@JDataRemotePush
public record PeerInfo(JObjectKey key, PeerId id, ByteString cert) implements JDataRemote, JDataRemoteDto {
    public PeerInfo(PeerId id, byte[] cert) {
        this(id.toJObjectKey(), id, ByteString.copyFrom(cert));
    }

    public X509Certificate parsedCert() {
        return CertificateTools.certFromBytes(cert.toByteArray());
    }

    @Override
    public String toString() {
        return "PeerInfo{" +
                "key=" + key +
                ", id=" + id +
                '}';
    }
}
