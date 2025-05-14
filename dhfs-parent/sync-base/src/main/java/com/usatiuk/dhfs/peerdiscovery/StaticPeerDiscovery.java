package com.usatiuk.dhfs.peerdiscovery;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Notifies PeerDiscoveryDirectory about statically configured peer addresses.
 */
@ApplicationScoped
public class StaticPeerDiscovery {
    private final List<IpPeerAddress> _peers;
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;

    public StaticPeerDiscovery(@ConfigProperty(name = "dhfs.peerdiscovery.static-peers") Optional<String> staticPeers) {
        var peers = staticPeers.orElse("");
        _peers = Arrays.stream(peers.split(",")).flatMap(e ->
                PeerAddrStringHelper.parse(e).stream()
        ).toList();
    }

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP, skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    public void discoverPeers() {
        for (var peer : _peers) {
            peerDiscoveryDirectory.notifyAddr(peer);
        }
    }
}
