package com.usatiuk.dhfs.objects.repository;

import lombok.Getter;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentRemoteHostsData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Getter
    private final UUID _selfUuid = UUID.randomUUID();
    @Getter
    private final AtomicLong _selfCounter = new AtomicLong();
    @Getter
    private final AtomicLong _irregularShutdownCounter = new AtomicLong();
    @Getter
    @Setter
    private X509Certificate _selfCertificate = null;
    @Getter
    @Setter
    private KeyPair _selfKeyPair = null;
}
