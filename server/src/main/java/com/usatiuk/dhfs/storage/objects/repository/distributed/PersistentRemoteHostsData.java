package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentRemoteHostsData implements Serializable {
    @Getter
    private final HashMap<UUID, HostInfo> _remoteHosts = new HashMap<>();

    @Getter
    private final UUID _selfUuid = UUID.randomUUID();

    @Getter
    @Setter
    private X509Certificate _selfCertificate = null;

    @Getter
    @Setter
    private KeyPair _selfKeyPair = null;

    @Getter
    private final AtomicLong _selfCounter = new AtomicLong();
}
