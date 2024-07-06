package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import jakarta.json.bind.annotation.JsonbCreator;
import jakarta.json.bind.annotation.JsonbProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.UUID;

@Getter
@AllArgsConstructor
public class HostInfo implements Serializable {
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
}
