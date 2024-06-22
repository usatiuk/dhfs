package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.DhfsObjectSyncGrpcGrpc;
import com.usatiuk.dhfs.objects.repository.distributed.PingRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class RemoteHostManager {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    SyncHandler syncHandler;

    TransientPeersState _transientPeersState = new TransientPeersState();

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
                return !s.getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE);
            });
            if (shouldTry) {
                Log.info("Trying to connect to " + host.getUuid());
                if (reachable(host)) {
                    handleConnectionSuccess(host.getUuid());
                }
            }
        }
    }

    public void handleConnectionSuccess(UUID host) {
        if (_transientPeersState.runReadLocked(d -> d.getStates().getOrDefault(
                host, new TransientPeersStateData.TransientPeerState(TransientPeersStateData.TransientPeerState.ConnectionState.NOT_SEEN)
        )).getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE)) return;
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeersStateData.TransientPeerState());
            var curState = d.getStates().get(host);
            curState.setState(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE);
            return null;
        });
        Log.info("Connected to " + host);
        syncHandler.doInitialResync(host);
    }

    public void handleConnectionError(UUID host) {
        Log.info("Lost connection to " + host);
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeersStateData.TransientPeerState());
            var curState = d.getStates().get(host);
            curState.setState(TransientPeersStateData.TransientPeerState.ConnectionState.UNREACHABLE);
            return null;
        });
    }


    @FunctionalInterface
    public interface ClientFunction<R> {
        R apply(DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    private <R> R withClient(String addr, int port, Optional<Long> timeout, ClientFunction<R> fn) {
        var channel = NettyChannelBuilder.forAddress(addr, port).negotiationType(NegotiationType.PLAINTEXT)
                .usePlaintext().build();
        var client = DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                .withMaxInboundMessageSize(Integer.MAX_VALUE);
        if (timeout.isPresent()) {
            client = client.withDeadlineAfter(timeout.get(), TimeUnit.MILLISECONDS);
        }
        try {
            return fn.apply(client);
        } finally {
            channel.shutdownNow();
        }
    }

    // FIXME:
    private boolean reachable(HostInfo hostInfo) {
        try {
            return withClient(hostInfo.getAddr(), hostInfo.getPort(), Optional.of(5000L /*ms*/), c -> {
                var ret = c.ping(PingRequest.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString()).build());
                if (!UUID.fromString(ret.getSelfUuid()).equals(hostInfo.getUuid())) {
                    throw new IllegalStateException("Ping selfUuid returned " + ret.getSelfUuid() + " but expected " + hostInfo.getUuid());
                }
                return true;
            });
        } catch (Exception ignored) {
            Log.info("Host " + hostInfo.getUuid() + " is unreachable: " + ignored.getMessage() + " " + ignored.getCause());
            return false;
        }
    }

    public boolean reachable(UUID host) {
        return reachable(persistentRemoteHostsService.getInfo(host));
    }

    public <R> R withClientAny(Collection<UUID> targets, ClientFunction<R> fn) {
        var shuffledList = new ArrayList<>(targets);
        Collections.shuffle(shuffledList);
        for (UUID target : shuffledList) {
            var hostinfo = persistentRemoteHostsService.getInfo(target);

            boolean shouldTry = _transientPeersState.runReadLocked(d -> {
                var res = d.getStates().get(hostinfo.getUuid());
                if (res == null) return true;
                return res.getState() == TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE;
            });

            if (!shouldTry) continue;

            try {
                return withClient(hostinfo.getAddr(), hostinfo.getPort(), Optional.empty(), fn);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().equals(Status.UNAVAILABLE)) {
                    Log.info("Host " + hostinfo.getUuid() + " is unreachable: " + e.getMessage());
                    handleConnectionError(hostinfo.getUuid());
                } else throw e;
            }
        }
        throw new IllegalStateException("No reachable targets!");
    }

    public <R> R withClient(UUID target, ClientFunction<R> fn) {
        var hostinfo = persistentRemoteHostsService.getInfo(target);
        return withClient(hostinfo.getAddr(), hostinfo.getPort(), Optional.empty(), fn);
    }

    public List<UUID> getAvailableHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> e.getValue().getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE))
                .map(Map.Entry::getKey).toList());
    }

    public List<UUID> getSeenHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> !e.getValue().getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.NOT_SEEN))
                .map(Map.Entry::getKey).toList());
    }

}
