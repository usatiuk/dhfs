package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.repository.peerdiscovery.PeerAddress;
import com.usatiuk.dhfs.objects.repository.peerdiscovery.PeerDiscoveryDirectory;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfoService;
import com.usatiuk.dhfs.objects.repository.peersync.api.PeerSyncApiClientDynamic;
import com.usatiuk.dhfs.objects.repository.peertrust.PeerTrustManager;
import com.usatiuk.dhfs.objects.repository.webapi.AvailablePeerInfo;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

@ApplicationScoped
public class PeerManager {
    private final ConcurrentMap<PeerId, PeerAddress> _states = new ConcurrentHashMap<>();
    // FIXME: Ideally not call them on every ping
    private final ArrayList<ConnectionEventListener> _connectedListeners = new ArrayList<>();
    private final ArrayList<ConnectionEventListener> _disconnectedListeners = new ArrayList<>();
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    RpcClientFactory rpcClientFactory;
    @Inject
    PeerSyncApiClientDynamic peerSyncApiClient;
    @Inject
    TransactionManager transactionManager;
    @Inject
    Transaction curTx;
    @Inject
    PeerTrustManager peerTrustManager;
    @ConfigProperty(name = "dhfs.objects.sync.ping.timeout")
    long pingTimeout;
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;
    private ExecutorService _heartbeatExecutor;

    // Note: keep priority updated with below
    void init(@Observes @Priority(600) StartupEvent event) throws IOException {
        _heartbeatExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Scheduled(every = "${dhfs.objects.reconnect_interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    public void tryConnectAll() {
        if (_heartbeatExecutor == null) return;
        try {
            _heartbeatExecutor.invokeAll(peerInfoService.getPeersNoSelf()
                    .stream()
                    .<Callable<Void>>map(host -> () -> {
                        try {
                            if (isReachable(host))
                                Log.trace("Heartbeat: " + host);
                            else
                                Log.debug("Trying to connect to " + host);
                            var bestAddr = selectBestAddress(host.id());
                            if (pingCheck(host, bestAddr))
                                handleConnectionSuccess(host, bestAddr);
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

    // Note: registrations should be completed with Priority < 600
    public void registerConnectEventListener(ConnectionEventListener listener) {
        synchronized (_connectedListeners) {
            _connectedListeners.add(listener);
        }
    }

    // Note: registrations should be completed with Priority < 600
    public void registerDisconnectEventListener(ConnectionEventListener listener) {
        synchronized (_disconnectedListeners) {
            _disconnectedListeners.add(listener);
        }
    }

    private void handleConnectionSuccess(PeerInfo host, PeerAddress address) {
        boolean wasReachable = isReachable(host);

//        boolean shouldSyncObj = persistentPeerDataService.markInitialObjSyncDone(host);
//        boolean shouldSyncOp = persistentPeerDataService.markInitialOpSyncDone(host);
//
//        if (shouldSyncObj)
//            syncHandler.pushInitialResyncObj(host);
//        if (shouldSyncOp)
//            syncHandler.pushInitialResyncOp(host);

        _states.put(host.id(), address);

        if (wasReachable) return;

        Log.info("Connected to " + host);

//        for (var l : _connectedListeners) {
//            l.apply(host);
//        }
    }

    public void handleConnectionError(PeerInfo host) {
        boolean wasReachable = isReachable(host);

        if (wasReachable)
            Log.info("Lost connection to " + host);

        _states.remove(host.id());

//        for (var l : _disconnectedListeners) {
//            l.apply(host);
//        }
    }

    // FIXME:
    private boolean pingCheck(PeerInfo host, PeerAddress address) {
        try {
            return rpcClientFactory.withObjSyncClient(host.id(), address, pingTimeout, c -> {
                var ret = c.ping(PingRequest.newBuilder().setSelfUuid(persistentPeerDataService.getSelfUuid().toString()).build());
                if (!UUID.fromString(ret.getSelfUuid()).equals(host.id().id())) {
                    throw new IllegalStateException("Ping selfUuid returned " + ret.getSelfUuid() + " but expected " + host.id());
                }
                return true;
            });
        } catch (Exception ignored) {
            Log.debug("Host " + host + " is unreachable: " + ignored.getMessage() + " " + ignored.getCause());
            return false;
        }
    }

    public boolean isReachable(PeerInfo host) {
        return _states.containsKey(host.id());
    }

    public PeerAddress getAddress(PeerId host) {
        return _states.get(host);
    }

    public List<PeerId> getAvailableHosts() {
        return _states.keySet().stream().toList();
    }

//    public List<UUID> getUnavailableHosts() {
//        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
//                .filter(e -> !e.getValue().isReachable())
//                .map(Map.Entry::getKey).toList());
//    }

//    public HostStateSnapshot getHostStateSnapshot() {
//        ArrayList<UUID> available = new ArrayList<>();
//        ArrayList<UUID> unavailable = new ArrayList<>();
//        _transientPeersState.runReadLocked(d -> {
//                    for (var v : d.getStates().entrySet()) {
//                        if (v.getValue().isReachable())
//                            available.add(v.getKey());
//                        else
//                            unavailable.add(v.getKey());
//                    }
//                    return null;
//                }
//        );
//        return new HostStateSnapshot(available, unavailable);
//    }

//    public void removeRemoteHost(UUID host) {
//        persistentPeerDataService.removeHost(host);
//        // Race?
//        _transientPeersState.runWriteLocked(d -> {
//            d.getStates().remove(host);
//            return null;
//        });
//    }

    private PeerAddress selectBestAddress(PeerId host) {
        return peerDiscoveryDirectory.getForPeer(host).stream().findFirst().orElseThrow();
    }

    public void addRemoteHost(PeerId host) {
        if (_states.containsKey(host)) {
            throw new IllegalStateException("Host " + host + " is already added");
        }

        transactionManager.run(() -> {
            if (peerInfoService.getPeerInfo(host).isPresent())
                throw new IllegalStateException("Host " + host + " is already added");

            var info = peerSyncApiClient.getSelfInfo(selectBestAddress(host));

            var cert = Base64.getDecoder().decode(info.cert());
            peerInfoService.putPeer(host, cert);
        });

        peerTrustManager.reloadTrustManagerHosts(
                transactionManager.run(() -> peerInfoService.getPeers().stream().toList())
        ); //FIXME:
    }

    public Collection<AvailablePeerInfo> getSeenButNotAddedHosts() {
        return transactionManager.run(() -> {
            return peerDiscoveryDirectory.getReachablePeers().stream().filter(p -> !peerInfoService.getPeerInfo(p).isPresent())
                    .map(p -> new AvailablePeerInfo(p.toString())).toList();
        });
    }

    @FunctionalInterface
    public interface ConnectionEventListener {
        void apply(UUID host);
    }

    public record HostStateSnapshot(List<UUID> available, List<UUID> unavailable) {
    }

}
