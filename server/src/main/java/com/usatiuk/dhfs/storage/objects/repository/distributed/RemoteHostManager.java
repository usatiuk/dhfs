package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.PingRequest;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.GetSelfInfoRequest;
import com.usatiuk.dhfs.storage.objects.repository.distributed.peersync.PersistentPeerInfo;
import com.usatiuk.dhfs.storage.objects.repository.distributed.webapi.AvailablePeerInfo;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class RemoteHostManager {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    SyncHandler syncHandler;

    @Inject
    RpcClientFactory rpcClientFactory;

    @ConfigProperty(name = "dhfs.objects.distributed.sync.ping.timeout")
    long pingTimeout;

    private final TransientPeersState _transientPeersState = new TransientPeersState();

    private final ConcurrentMap<UUID, TransientPeerState> _seenHostsButNotAdded = new ConcurrentHashMap<>();

    boolean _initialized = false;

    void init(@Observes @Priority(350) StartupEvent event) throws IOException {
        _initialized = true;
    }

    void shutdown(@Observes @Priority(250) ShutdownEvent event) throws IOException {
    }

    @Scheduled(every = "${dhfs.objects.distributed.reconnect_interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    public void tryConnectAll() {
        if (!_initialized) return;
        for (var host : persistentRemoteHostsService.getHosts()) {
            try {
                var shouldTry = _transientPeersState.runReadLocked(d -> {
                    var s = d.getStates().get(host.getUuid());
                    if (s == null) return true;
                    return !s.getState().equals(TransientPeerState.ConnectionState.REACHABLE) && s.getAddr() != null;
                });
                if (shouldTry) {
                    Log.info("Trying to connect to " + host.getUuid());
                    if (pingCheck(host.getUuid())) {
                        handleConnectionSuccess(host.getUuid());
                    }
                }
            } catch (Exception e) {
                Log.error("Failed to connect to " + host.getUuid(), e);
                continue;
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
            return rpcClientFactory.withObjSyncClient(host.toString(), state.getAddr(), state.getSecurePort(), pingTimeout, c -> {
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

    public void notifyAddr(UUID host, String addr, Integer port, Integer securePort) {
        if (host.equals(persistentRemoteHostsService.getSelfUuid())) {
            return;
        }

        var state = new TransientPeerState();
        state.setAddr(addr);
        state.setPort(port);
        state.setSecurePort(securePort);

        if (!persistentRemoteHostsService.existsHost(host)) {
            _seenHostsButNotAdded.put(host, state);
            // Needed for tests
            Log.trace("Ignoring new address from unknown host " + ": addr=" + addr + " port=" + port);
            return;
        }

        _transientPeersState.runWriteLocked(d -> {
//            Log.trace("Updating connection info for " + host + ": addr=" + addr + " port=" + port);
            d.getStates().putIfAbsent(host, new TransientPeerState()); // FIXME:? set reachable here?
            d.getStates().get(host).setAddr(addr);
            d.getStates().get(host).setPort(port);
            d.getStates().get(host).setSecurePort(securePort);
            return null;
        });
    }

    public void addRemoteHost(UUID host) {
        if (!_seenHostsButNotAdded.containsKey(host)) {
            throw new IllegalStateException("Host " + host + " is not seen");
        }
        if (persistentRemoteHostsService.existsHost(host)) {
            throw new IllegalStateException("Host " + host + " is already added");
        }

        var state = _seenHostsButNotAdded.get(host);

        // FIXME: race?

        var info = rpcClientFactory.withPeerSyncClient(state.getAddr(), state.getPort(), 10000L, c -> {
            return c.getSelfInfo(GetSelfInfoRequest.getDefaultInstance());
        });

        try {
            persistentRemoteHostsService.addHost(
                    new PersistentPeerInfo(UUID.fromString(info.getUuid()), CertificateTools.certFromBytes(info.getCert().toByteArray())));
            Log.info("Added host: " + host.toString());
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<AvailablePeerInfo> getSeenButNotAddedHosts() {
        return _seenHostsButNotAdded.entrySet().stream()
                .filter(e -> !persistentRemoteHostsService.existsHost(e.getKey()))
                .map(e -> new AvailablePeerInfo(e.getKey().toString(), e.getValue().getAddr(), e.getValue().getPort()))
                .toList();
    }

}
