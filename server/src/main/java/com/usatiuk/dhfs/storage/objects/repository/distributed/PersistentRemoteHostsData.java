package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

public class PersistentRemoteHostsData implements Serializable {
    @Getter
    private final HashMap<UUID, HostInfo> _remoteHosts = new HashMap<>();

    @Getter
    private final UUID _selfUuid = UUID.randomUUID();
}
