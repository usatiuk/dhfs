package com.usatiuk.dhfs.peerdiscovery.local;


import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "dhfs.objects.peerdiscovery")
public interface LocalPeerDiscoveryConfig {
    @WithDefault("42262")
    int port();

    @WithDefault("true")
    boolean broadcast();

    String interval();
}
