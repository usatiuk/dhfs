package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PeerId;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Peer discovery directory collects known peer addresses, and automatically cleans up old entries.
 */
@ApplicationScoped
public class PeerDiscoveryDirectory {
    private final MultiValuedMap<PeerId, PeerEntry> _entries = new HashSetValuedHashMap<>();
    @ConfigProperty(name = "dhfs.peerdiscovery.timeout")
    long timeout;

    /**
     * Notifies the directory about a new address for a peer.
     * If the address is already known, it updates the last seen time.
     *
     * @param addr the new address
     */
    public void notifyAddr(PeerAddress addr) {
        Log.tracev("New address {0}", addr);
        synchronized (_entries) {
            var peer = addr.peer();
            _entries.removeMapping(peer, new PeerEntry(addr, 0));
            _entries.put(peer, new PeerEntry(addr, System.currentTimeMillis()));
        }
    }

    /**
     * Returns a collection of addresses for a given peer.
     * Cleans up old entries that are no longer reachable.
     *
     * @param peer the peer ID
     * @return a collection of addresses for the peer
     */
    public Collection<PeerAddress> getForPeer(PeerId peer) {
        synchronized (_entries) {
            long curTime = System.currentTimeMillis();
            if (_entries.asMap().get(peer) == null) {
                return List.of();
            }
            var partitioned = _entries.asMap().get(peer).stream()
                    .collect(Collectors.partitioningBy(e -> e.lastSeen() + timeout < curTime));
            for (var entry : partitioned.get(true)) {
                _entries.removeMapping(peer, entry);
            }
            return partitioned.get(false).stream().map(PeerEntry::addr).toList();
        }
    }

    /**
     * Returns a collection of reachable peers.
     * Cleans up old entries that are no longer reachable.
     *
     * @return a collection of reachable peers
     */
    public Collection<PeerId> getReachablePeers() {
        synchronized (_entries) {
            long curTime = System.currentTimeMillis();
            var partitioned = _entries.entries().stream()
                    .collect(Collectors.partitioningBy(e -> e.getValue().lastSeen() + timeout < curTime));
            for (var entry : partitioned.get(true)) {
                _entries.removeMapping(entry.getKey(), entry.getValue());
            }
            return partitioned.get(false).stream().map(Map.Entry::getKey).collect(Collectors.toUnmodifiableSet());
        }
    }

    private record PeerEntry(PeerAddress addr, long lastSeen) {
        public PeerEntry withLastSeen(long lastSeen) {
            return new PeerEntry(addr, lastSeen);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            PeerEntry peerEntry = (PeerEntry) o;
            return Objects.equals(addr, peerEntry.addr);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(addr);
        }
    }
}
