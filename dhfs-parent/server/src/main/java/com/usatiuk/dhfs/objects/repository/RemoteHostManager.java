package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.repository.peersync.PeerSyncApiClientDynamic;
import com.usatiuk.dhfs.objects.repository.peersync.PersistentPeerInfo;
import com.usatiuk.dhfs.objects.repository.webapi.AvailablePeerInfo;
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
import java.util.*;
import java.util.concurrent.*;

@ApplicationScoped
public class RemoteHostManager {
    private final TransientPeersState _transientPeersState = new TransientPeersState();
    private final ConcurrentMap<UUID, TransientPeerState> _seenHostsButNotAdded = new ConcurrentHashMap<>();
    // FIXME: Ideally not call them on every ping
    private final ArrayList<ConnectionEventListener> _connectedListeners = new ArrayList<>();
    private final ArrayList<ConnectionEventListener> _disconnectedListeners = new ArrayList<>();
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;
    @Inject
    SyncHandler syncHandler;
    @Inject
    RpcClientFactory rpcClientFactory;
    @Inject
    PeerSyncApiClientDynamic peerSyncApiClient;
    @ConfigProperty(name = "dhfs.objects.sync.ping.timeout")
    long pingTimeout;
    private ExecutorService _heartbeatExecutor;
    boolean _initialized = false;

    // Note: keep priority updated with below
    void init(@Observes @Priority(350) StartupEvent event) throws IOException {
        _heartbeatExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Note: newly added hosts aren't in _transientPeersState
        // but that's ok as they don't have initialSyncDone set
        for (var h : persistentRemoteHostsService.getHostsUuid())
            _transientPeersState.runWriteLocked(d -> d.get(h));

        _initialized = true;
    }

    void shutdown(@Observes @Priority(250) ShutdownEvent event) throws IOException {
    }

    @Scheduled(every = "${dhfs.objects.reconnect_interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    public void tryConnectAll() {
        if (!_initialized) return;
        try {
            _heartbeatExecutor.invokeAll(persistentRemoteHostsService.getHostsUuid().stream()
                    .<Callable<Void>>map(host -> () -> {
                        try {
                            if (isReachable(host))
                                Log.debug("Heartbeat: " + host);
                            else
                                Log.info("Trying to connect to " + host);
                            if (pingCheck(host))
                                handleConnectionSuccess(host);
                            else
                                handleConnectionError(host);
                        } catch (Exception e) {
                            Log.error("Failed to connect to " + host, e);
                        }
                        return null;
                    }).toList(), 30, TimeUnit.SECONDS); //FIXME:
        } catch (InterruptedException iex) {
            Log.error("Heartbeat was interrupted");
        }
    }

    @FunctionalInterface
    public interface ConnectionEventListener {
        void apply(UUID host);
    }

    // Note: registrations should be completed with Priority < 350
    public void registerConnectEventListener(ConnectionEventListener listener) {
        synchronized (_connectedListeners) {
            _connectedListeners.add(listener);
        }
    }

    // Note: registrations should be completed with Priority < 350
    public void registerDisconnectEventListener(ConnectionEventListener listener) {
        synchronized (_disconnectedListeners) {
            _disconnectedListeners.add(listener);
        }
    }

    public void handleConnectionSuccess(UUID host) {
        boolean wasReachable = isReachable(host);

        _transientPeersState.runWriteLocked(d -> {
            d.get(host).setReachable(true);
            return null;
        });

        for (var l : _connectedListeners) {
            l.apply(host);
        }

        if (wasReachable) return;

        Log.info("Connected to " + host);

        if (persistentRemoteHostsService.markInitialSyncDone(host))
            syncHandler.doInitialResync(host);
    }

    public void handleConnectionError(UUID host) {
        boolean wasReachable = isReachable(host);

        if (wasReachable)
            Log.info("Lost connection to " + host);

        _transientPeersState.runWriteLocked(d -> {
            d.get(host).setReachable(false);
            return null;
        });

        for (var l : _disconnectedListeners) {
            l.apply(host);
        }
    }

    // FIXME:
    private boolean pingCheck(UUID host) {
        TransientPeerState state = _transientPeersState.runReadLocked(s -> s.getCopy(host));

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
        return _transientPeersState.runReadLocked(d -> d.get(host).isReachable());
    }

    public TransientPeerState getTransientState(UUID host) {
        return _transientPeersState.runReadLocked(d -> d.getCopy(host));
    }

    public List<UUID> getAvailableHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> e.getValue().isReachable())
                .map(Map.Entry::getKey).toList());
    }

    public List<UUID> getUnavailableHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> !e.getValue().isReachable())
                .map(Map.Entry::getKey).toList());
    }

    public record HostStateSnapshot(List<UUID> available, List<UUID> unavailable) {
    }

    public HostStateSnapshot getHostStateSnapshot() {
        ArrayList<UUID> available = new ArrayList<>();
        ArrayList<UUID> unavailable = new ArrayList<>();
        _transientPeersState.runReadLocked(d -> {
                    for (var v : d.getStates().entrySet()) {
                        if (v.getValue().isReachable())
                            available.add(v.getKey());
                        else
                            unavailable.add(v.getKey());
                    }
                    return null;
                }
        );
        return new HostStateSnapshot(available, unavailable);
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
            var prev = _seenHostsButNotAdded.put(host, state);
            // Needed for tests
            if (prev == null)
                Log.debug("Ignoring new address from unknown host " + ": addr=" + addr + " port=" + port);
            return;
        } else {
            _seenHostsButNotAdded.remove(host);
        }

        _transientPeersState.runWriteLocked(d -> {
//            Log.trace("Updating connection info for " + host + ": addr=" + addr + " port=" + port);
            d.get(host).setAddr(addr);
            d.get(host).setPort(port);
            d.get(host).setSecurePort(securePort);
            return null;
        });
    }

    public void removeRemoteHost(UUID host) {
        persistentRemoteHostsService.removeHost(host);
        // Race?
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().remove(host);
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

        var info = peerSyncApiClient.getSelfInfo(state.getAddr(), state.getPort());

        try {
            persistentRemoteHostsService.addHost(
                    new PersistentPeerInfo(UUID.fromString(info.selfUuid()),
                            CertificateTools.certFromBytes(Base64.getDecoder().decode(info.cert()))));
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
