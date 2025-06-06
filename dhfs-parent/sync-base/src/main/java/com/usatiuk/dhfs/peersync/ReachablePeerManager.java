package com.usatiuk.dhfs.peersync;

import com.usatiuk.dhfs.peerdiscovery.PeerAddress;
import com.usatiuk.dhfs.peerdiscovery.PeerDiscoveryDirectory;
import com.usatiuk.dhfs.peersync.api.ApiPeerInfo;
import com.usatiuk.dhfs.peersync.api.PeerSyncApiClientDynamic;
import com.usatiuk.dhfs.peertrust.PeerTrustManager;
import com.usatiuk.dhfs.remoteobj.SyncHandler;
import com.usatiuk.dhfs.repository.PingRequest;
import com.usatiuk.dhfs.rpc.RpcClientFactory;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Nullable;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Handles connections to known peers in the cluster.
 */
@ApplicationScoped
public class ReachablePeerManager {
    private final ConcurrentMap<PeerId, PeerAddress> _states = new ConcurrentHashMap<>();
    private final Collection<PeerConnectedEventListener> _connectedListeners;
    private final Collection<PeerDisconnectedEventListener> _disconnectedListeners;
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
    @ConfigProperty(name = "dhfs.sync.cert-check", defaultValue = "true")
    boolean certCheck;
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;
    @Inject
    SyncHandler syncHandler;
    private ExecutorService _heartbeatExecutor;

    public ReachablePeerManager(Instance<PeerConnectedEventListener> connectedListeners, Instance<PeerDisconnectedEventListener> disconnectedListeners) {
        _connectedListeners = List.copyOf(connectedListeners.stream().toList());
        _disconnectedListeners = List.copyOf(disconnectedListeners.stream().toList());
    }

    void init(@Observes @Priority(600) StartupEvent event) throws IOException {
        _heartbeatExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Scheduled(every = "${dhfs.objects.reconnect_interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP, skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    @Blocking
    public void tryConnectAll() {
        if (_heartbeatExecutor == null) return;
        try {
            var peers = peerInfoService.getPeersNoSelf();
            var pids = peers.stream().map(com.usatiuk.dhfs.peersync.PeerInfo::id).toList();

            List<PeerId> stale = _states.keySet().stream().filter(p -> !pids.contains(p)).toList();
            stale.forEach(_states.keySet()::remove);

            _heartbeatExecutor.invokeAll(peers
                    .stream()
                    .<Callable<Void>>map(host -> () -> {
                        try {
                            if (isReachable(host))
                                Log.tracev("Heartbeat: {0}", host);
                            else
                                Log.debugv("Trying to connect to {0}", host);
                            var bestAddr = selectBestAddress(host.id()).orElse(null);
                            if (bestAddr != null && pingCheck(host, bestAddr))
                                handleConnectionSuccess(host, bestAddr);
                            else
                                handleConnectionError(host);
                        } catch (Exception e) {
                            Log.error("Failed to connect to " + host.key(), e);
                        }
                        return null;
                    }).toList(), 30, TimeUnit.SECONDS); //FIXME:
        } catch (InterruptedException iex) {
            Log.error("Heartbeat was interrupted");
        }
    }

    private void handleConnectionSuccess(com.usatiuk.dhfs.peersync.PeerInfo host, PeerAddress address) {
        boolean wasReachable = isReachable(host);

        boolean shouldSync = !persistentPeerDataService.isInitialSyncDone(host.id());

        if (shouldSync) {
            syncHandler.doInitialSync(host.id());
            persistentPeerDataService.markInitialSyncDone(host.id());
        }

        _states.put(host.id(), address);

        if (wasReachable)
            return;

        Log.infov("Connected to {0}", host);

        for (var l : _connectedListeners) {
            l.handlePeerConnected(host.id());
        }
    }

    public void handleConnectionError(PeerId host) {
        boolean wasReachable = isReachable(host);

        if (wasReachable)
            Log.infov("Lost connection to {0}", host);

        _states.remove(host);

        for (var l : _disconnectedListeners) {
            l.handlePeerDisconnected(host);
        }
    }

    private void handleConnectionError(com.usatiuk.dhfs.peersync.PeerInfo host) {
        handleConnectionError(host.id());
    }

    private boolean pingCheck(com.usatiuk.dhfs.peersync.PeerInfo host, PeerAddress address) {
        try {
            return rpcClientFactory.withObjSyncClient(host.id(), address, pingTimeout, (peer, c) -> {
                c.ping(PingRequest.getDefaultInstance());
                return true;
            });
        } catch (Exception ignored) {
            Log.debugv("Host {0} is unreachable: {1}, {2}", host, ignored.getMessage(), ignored.getCause());
            return false;
        }
    }

    /**
     * Checks if the given host is reachable.
     *
     * @param host the host to check
     * @return true if the host is reachable, false otherwise
     */
    public boolean isReachable(PeerId host) {
        return _states.containsKey(host);
    }

    /**
     * Checks if the given host is reachable.
     *
     * @param host the host to check
     * @return true if the host is reachable, false otherwise
     */
    public boolean isReachable(com.usatiuk.dhfs.peersync.PeerInfo host) {
        return isReachable(host.id());
    }

    /**
     * Gets the address of the given host.
     *
     * @param host the host to get the address for
     * @return the address of the host, or null if not reachable
     */
    public PeerAddress getAddress(PeerId host) {
        return _states.get(host);
    }

    /**
     * Gets the ids of all reachable hosts.
     *
     * @return a list of ids of all reachable hosts
     */
    public List<PeerId> getAvailableHosts() {
        return _states.keySet().stream().toList();
    }

    /**
     * Gets a snapshot of current state of the connected (and not connected) peers
     *
     * @return information about all connected/disconnected peers
     */
    public HostStateSnapshot getHostStateSnapshot() {
        return transactionManager.run(() -> {
            var partition = peerInfoService.getPeersNoSelf().stream().map(com.usatiuk.dhfs.peersync.PeerInfo::id)
                    .collect(Collectors.partitioningBy(this::isReachable));
            return new HostStateSnapshot(partition.get(true), partition.get(false));
        });
    }

    /**
     * Removes the given host from the cluster
     *
     * @param peerId the id of the host to remove
     */
    public void removeRemoteHost(PeerId peerId) {
        transactionManager.run(() -> {
            peerInfoService.removePeer(peerId);
        });
    }

    /**
     * Selects the best address for the given host.
     * The address is selected based on the type of the address. (with e.g. LAN address preferred over WAN)
     *
     * @param host the host to select the address for
     * @return the best address for the host, or null if not reachable
     */
    public Optional<PeerAddress> selectBestAddress(PeerId host) {
        return peerDiscoveryDirectory.getForPeer(host).stream().min(Comparator.comparing(PeerAddress::type));
    }

    /**
     * Call the given peer and get its information.
     *
     * @param host the peer to get the information for
     * @return the information about the peer
     */
    private ApiPeerInfo getInfo(PeerId host) {
        return transactionManager.run(() -> {
            if (peerInfoService.getPeerInfo(host).isPresent())
                throw new IllegalStateException("Host " + host + " is already added");

            var addr = selectBestAddress(host).orElseThrow(() -> new IllegalStateException("Host " + host + " is unreachable"));
            var info = peerSyncApiClient.getSelfInfo(addr);

            return info;
        });
    }

    /**
     * Adds the given peer to the cluster.
     * The certificate provided is verified against the one peer is using right now.
     *
     * @param host the peer to add
     * @param cert the certificate of the peer
     */
    public void addRemoteHost(PeerId host, @Nullable String cert) {
        transactionManager.run(() -> {
            var info = getInfo(host);

            var certGot = Base64.getDecoder().decode(info.cert());
            if (certCheck) {
                var certApi = Base64.getDecoder().decode(cert);
                if (!Arrays.equals(certGot, certApi))
                    throw new IllegalStateException("Host " + host + " has different cert");
            }
            peerInfoService.putPeer(host, certGot);
        });

        peerTrustManager.reloadTrustManagerHosts(transactionManager.run(() -> peerInfoService.getPeers().stream().toList())); //FIXME:
    }

    /**
     * Gets the information about all reachable peers that are not added to the cluster.
     *
     * @return a collection of pairs of peer id and peer info
     */
    public Collection<Pair<PeerId, ApiPeerInfo>> getSeenButNotAddedHosts() {
        return transactionManager.run(() -> {
            return peerDiscoveryDirectory.getReachablePeers().stream().filter(p -> !peerInfoService.getPeerInfo(p).isPresent())
                    .flatMap(p -> {
                        try {
                            return Stream.of(Pair.of(p, getInfo(p)));
                        } catch (Exception e) {
                            Log.warn("Error getting peer info for " + p, e);
                            return Stream.empty();
                        }
                    }).toList();
        });
    }

    public record HostStateSnapshot(Collection<PeerId> available, Collection<PeerId> unavailable) {
    }

}
