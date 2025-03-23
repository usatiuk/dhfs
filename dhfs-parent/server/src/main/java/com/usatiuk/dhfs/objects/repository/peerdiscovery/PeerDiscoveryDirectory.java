package com.usatiuk.dhfs.objects.repository.peerdiscovery;

import com.usatiuk.dhfs.objects.PeerId;
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

@ApplicationScoped
public class PeerDiscoveryDirectory {
    private final MultiValuedMap<PeerId, PeerEntry> _entries = new HashSetValuedHashMap<>();
    @ConfigProperty(name = "dhfs.peerdiscovery.timeout")
    long timeout;

    public void notifyAddr(PeerAddress addr) {
        Log.tracev("New address {0}", addr);
        synchronized (_entries) {
            var peer = addr.peer();
            _entries.removeMapping(peer, new PeerEntry(addr, 0));
            _entries.put(peer, new PeerEntry(addr, System.currentTimeMillis()));
        }
    }

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
