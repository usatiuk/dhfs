package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.persistence.ChunkDataP;
import com.usatiuk.dhfs.objects.persistence.PeerInfoP;
import com.usatiuk.dhfs.objects.repository.CertificateTools;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public record PeerInfo(JObjectKey key, PeerId id, byte[] cert) implements JDataRemote {
    public PeerInfo(PeerId id, byte[] cert) {
        this(id.toJObjectKey(), id, cert);
    }

    public X509Certificate parsedCert() {
        try {
            return CertificateTools.certFromBytes(cert);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }
}
