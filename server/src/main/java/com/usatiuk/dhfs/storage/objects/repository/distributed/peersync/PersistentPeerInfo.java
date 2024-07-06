package com.usatiuk.dhfs.storage.objects.repository.distributed.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import com.usatiuk.dhfs.storage.files.conflicts.NotImplementedConflictResolver;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Getter
@AllArgsConstructor
public class PersistentPeerInfo extends JObjectData {
    private final UUID _uuid;
    private final X509Certificate _certificate;

    public PeerInfo toPeerInfo() {
        try {
            return PeerInfo.newBuilder().setUuid(_uuid.toString())
                    .setCert(ByteString.copyFrom(_certificate.getEncoded())).build();
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return _uuid.toString();
    }

    @Override
    public boolean pushResolution() {
        return true;
    }

    public boolean assumeUnique() {
        return true;
    }

    // FIXME: Maybe check the certs?
    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NotImplementedConflictResolver.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of();
    }
}
