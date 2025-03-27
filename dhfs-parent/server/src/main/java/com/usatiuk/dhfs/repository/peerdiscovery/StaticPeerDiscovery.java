package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.PeerId;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@ApplicationScoped
public class StaticPeerDiscovery {
    private final List<IpPeerAddress> _peers;
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;

    public StaticPeerDiscovery(@ConfigProperty(name = "dhfs.peerdiscovery.static-peers") Optional<String> staticPeers) {
        var peers = staticPeers.orElse("");
        _peers = Arrays.stream(peers.split(",")).flatMap(e ->
        {
            if (e.isEmpty()) {
                return Stream.of();
            }
            var split = e.split(":");
            try {
                return Stream.of(new IpPeerAddress(PeerId.of(split[0]), PeerAddressType.LAN, InetAddress.getByName(split[1]),
                        Integer.parseInt(split[2]), Integer.parseInt(split[3])));
            } catch (UnknownHostException ex) {
                throw new RuntimeException(ex);
            }
        }).toList();
    }

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void discoverPeers() {
        for (var peer : _peers) {
            peerDiscoveryDirectory.notifyAddr(peer);
        }
    }
}
