package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Notified PeerDiscoveryDirectory about manually added peer addresses.
 */
@ApplicationScoped
public class PersistentStaticPeerDiscovery {
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP, skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    public void discoverPeers() {
        var addrs = persistentPeerDataService.getPersistentPeerAddresses();
        for (var addr : addrs) {
            peerDiscoveryDirectory.notifyAddr(addr);
        }
    }
}
