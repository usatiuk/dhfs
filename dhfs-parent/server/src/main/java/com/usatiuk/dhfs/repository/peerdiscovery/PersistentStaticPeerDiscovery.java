package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.repository.PersistentPeerDataService;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class PersistentStaticPeerDiscovery {
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void discoverPeers() {
        var addrs = persistentPeerDataService.getPersistentPeerAddresses();
        for (var addr : addrs) {
            peerDiscoveryDirectory.notifyAddr(addr);
        }
    }
}
