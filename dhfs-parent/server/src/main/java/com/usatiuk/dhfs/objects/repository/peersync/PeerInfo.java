package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.repository.CertificateTools;
import org.pcollections.HashTreePSet;
import org.pcollections.PCollection;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public record PeerInfo(JObjectKey key, PCollection<JObjectKey> refsFrom, boolean frozen, PeerId id,
                       byte[] cert) implements JDataRefcounted, JDataRemote {
    public PeerInfo(PeerId id, byte[] cert) {
        this(id.toJObjectKey(), HashTreePSet.empty(), false, id, cert);
    }

    @Override
    public JDataRefcounted withRefsFrom(PCollection<JObjectKey> refs) {
        return new PeerInfo(key, refs, frozen, id, cert);
    }

    @Override
    public JDataRefcounted withFrozen(boolean frozen) {
        return new PeerInfo(key, refsFrom, frozen, id, cert);
    }

    public X509Certificate parsedCert() {
        try {
            return CertificateTools.certFromBytes(cert);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }
}
