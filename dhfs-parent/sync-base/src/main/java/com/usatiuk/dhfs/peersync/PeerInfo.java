package com.usatiuk.dhfs.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.peertrust.CertificateTools;
import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;

import java.security.cert.X509Certificate;

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
