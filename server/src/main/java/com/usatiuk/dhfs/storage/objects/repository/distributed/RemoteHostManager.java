package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.PingRequest;
import com.usatiuk.dhfs.storage.objects.repository.distributed.peersync.PeerSyncClient;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class RemoteHostManager {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    SyncHandler syncHandler;

    @Inject
    RpcClientFactory rpcClientFactory;

    @Inject
    PeerSyncClient peerSyncClient;

    private final TransientPeersState _transientPeersState = new TransientPeersState();

    void init(@Observes @Priority(350) StartupEvent event) throws IOException {
    }

    void shutdown(@Observes @Priority(250) ShutdownEvent event) throws IOException {
    }

    @Scheduled(every = "2s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    public void tryConnectAll() {
        for (var host : persistentRemoteHostsService.getHosts()) {
            var shouldTry = _transientPeersState.runReadLocked(d -> {
                var s = d.getStates().get(host.getUuid());
                if (s == null) return true;
                return !s.getState().equals(TransientPeerState.ConnectionState.REACHABLE);
            });
            if (shouldTry) {
                Log.info("Trying to connect to " + host.getUuid());
                if (pingCheck(host.getUuid())) {
                    handleConnectionSuccess(host.getUuid());
                }
            }
        }
    }

    public void handleConnectionSuccess(UUID host) {
        if (_transientPeersState.runReadLocked(d -> d.getStates().getOrDefault(
                host, new TransientPeerState(TransientPeerState.ConnectionState.NOT_SEEN)
        )).getState().equals(TransientPeerState.ConnectionState.REACHABLE)) return;
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeerState());
            var curState = d.getStates().get(host);
            curState.setState(TransientPeerState.ConnectionState.REACHABLE);
            return null;
        });
        Log.info("Connected to " + host);
        syncHandler.doInitialResync(host);
        peerSyncClient.syncPeersOne(host);
    }

    public void handleConnectionError(UUID host) {
        Log.info("Lost connection to " + host);
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeerState());
            var curState = d.getStates().get(host);
            curState.setState(TransientPeerState.ConnectionState.UNREACHABLE);
            return null;
        });
    }

    // FIXME:
    private boolean pingCheck(UUID host) {
        TransientPeerState state = _transientPeersState.runReadLocked(s -> s.getStates().get(host));
        if (state == null) return false;
        try {
            return rpcClientFactory.withObjSyncClient(state.getAddr(), state.getPort(), Optional.of(5000L /*ms*/), c -> {
                var ret = c.ping(PingRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).build());
                if (!UUID.fromString(ret.getSelfUuid()).equals(host)) {
                    throw new IllegalStateException("Ping selfUuid returned " + ret.getSelfUuid() + " but expected " + host);
                }
                return true;
            });
        } catch (Exception ignored) {
            Log.info("Host " + host + " is unreachable: " + ignored.getMessage() + " " + ignored.getCause());
            return false;
        }
    }

    public boolean isReachable(UUID host) {
        return _transientPeersState.runReadLocked(d -> {
            var res = d.getStates().get(host);
            return res.getState() == TransientPeerState.ConnectionState.REACHABLE;
        });
    }

    public TransientPeerState getTransientState(UUID host) {
        return _transientPeersState.runReadLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeerState());
            return d.getStates().get(host);
        });
    }

    public List<UUID> getAvailableHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> e.getValue().getState().equals(TransientPeerState.ConnectionState.REACHABLE))
                .map(Map.Entry::getKey).toList());
    }

    public List<UUID> getSeenHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> !e.getValue().getState().equals(TransientPeerState.ConnectionState.NOT_SEEN))
                .map(Map.Entry::getKey).toList());
    }

    public void notifyAddr(UUID host, String addr, Integer port) {
        _transientPeersState.runWriteLocked(d -> {
            Log.info("Updating connection info for " + host + ": addr=" + addr + " port=" + port);
            d.getStates().putIfAbsent(host, new TransientPeerState());
            d.getStates().get(host).setAddr(addr);
            d.getStates().get(host).setPort(port);
            return null;
        });
    }

}
