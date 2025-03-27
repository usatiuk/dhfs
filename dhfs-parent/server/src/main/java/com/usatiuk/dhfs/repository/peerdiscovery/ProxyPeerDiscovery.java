package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.repository.PeerManager;
import com.usatiuk.dhfs.repository.ProxyDiscoveryServiceClient;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ProxyPeerDiscovery {
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;
    @Inject
    ProxyDiscoveryServiceClient proxyDiscoveryServiceClient;
    @Inject
    PeerManager peerManager;

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void discoverPeers() {
        Log.tracev("Discovering proxy peers");
        for (var p : peerManager.getDirectAvailableHosts()) {
            var got = proxyDiscoveryServiceClient.getAvailablePeers(p);
            Log.tracev("Asked {0} for peers, got {1}", p, got);
            for (var peer : got) {
                peerDiscoveryDirectory.notifyAddr(
                        new ProxyPeerAddress(peer, PeerAddressType.PROXY, p)
                );
            }
        }
    }
}
