package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;

public class PersistentRemoteHostsData implements Serializable {
    @Getter
    private final HashMap<String, HostInfo> _remoteHosts = new HashMap<>();
}
