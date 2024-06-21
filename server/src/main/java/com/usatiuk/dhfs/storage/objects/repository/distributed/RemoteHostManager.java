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
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@ApplicationScoped
public class RemoteHostManager {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    TransientPeersState _transientPeersState = new TransientPeersState();

    void init(@Observes @Priority(350) StartupEvent event) throws IOException {
        tryConnectAll();
    }

    void shutdown(@Observes @Priority(250) ShutdownEvent event) throws IOException {
    }

    @Scheduled(every = "2s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @Blocking
    public void tryConnectAll() {
        for (var host : persistentRemoteHostsService.getHosts()) {
            var shouldTry = _transientPeersState.runReadLocked(d -> {
                var s = d.getStates().get(host.getName());
                if (s == null) return true;
                return !s.getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE);
            });
            if (shouldTry) {
                Log.info("Trying to connect to " + host.getName());
                if (reachable(host)) {
                    Log.info("Connected to " + host);
                    handleConnectionSuccess(host.getName());
                }
            }
        }
    }

    private final ArrayList<Function<String, Void>> _connectionSuccessHandlers = new ArrayList<>();
    private final ArrayList<Function<String, Void>> _connectionErrorHandlers = new ArrayList<>();

    public void handleConnectionSuccess(String host) {
        if (_transientPeersState.runReadLocked(d -> d.getStates().getOrDefault(
                host, new TransientPeersStateData.TransientPeerState(TransientPeersStateData.TransientPeerState.ConnectionState.NOT_SEEN)
        )).getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE)) return;
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeersStateData.TransientPeerState());
            var curState = d.getStates().get(host);
            curState.setState(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE);
            return null;
        });
        for (var h : _connectionSuccessHandlers) {
            h.apply(host);
        }
    }

    public void handleConnectionError(String host) {
        _transientPeersState.runWriteLocked(d -> {
            d.getStates().putIfAbsent(host, new TransientPeersStateData.TransientPeerState());
            var curState = d.getStates().get(host);
            curState.setState(TransientPeersStateData.TransientPeerState.ConnectionState.UNREACHABLE);
            return null;
        });
        for (var h : _connectionErrorHandlers) {
            h.apply(host);
        }
    }

    public void addConnectionSuccessHandler(Function<String, Void> handler) {
        _connectionSuccessHandlers.add(handler);
    }

    public void addConnectionErrorHandler(Function<String, Void> handler) {
        _connectionErrorHandlers.add(handler);
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
                var ret = c.ping(PingRequest.newBuilder().setSelfname(selfname).build());
                if (!ret.getSelfname().equals(hostInfo.getName())) {
                    throw new IllegalStateException("Ping selfname returned " + ret.getSelfname() + " but expected " + hostInfo.getName());
                }
                return true;
            });
        } catch (Exception ignored) {
            Log.info("Host " + hostInfo.getName() + " is unreachable: " + ignored.getMessage() + " " + ignored.getCause());
            return false;
        }
    }

    public boolean reachable(String host) {
        return reachable(persistentRemoteHostsService.getInfo(host));
    }

    public <R> R withClientAny(Collection<String> targets, ClientFunction<R> fn) {
        var shuffledList = new ArrayList<>(targets);
        Collections.shuffle(shuffledList);
        for (String target : shuffledList) {
            var hostinfo = persistentRemoteHostsService.getInfo(target);

            boolean shouldTry = _transientPeersState.runReadLocked(d -> {
                var res = d.getStates().get(hostinfo.getName());
                if (res == null) return true;
                return res.getState() == TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE;
            });

            if (!shouldTry) continue;

            try {
                return withClient(hostinfo.getAddr(), hostinfo.getPort(), Optional.empty(), fn);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().equals(Status.UNAVAILABLE)) {
                    Log.info("Host " + hostinfo.getName() + " is unreachable: " + e.getMessage());
                    handleConnectionError(hostinfo.getName());
                } else throw e;
            }
        }
        throw new IllegalStateException("No reachable targets!");
    }

    public <R> R withClient(String target, ClientFunction<R> fn) {
        var hostinfo = persistentRemoteHostsService.getInfo(target);
        return withClient(hostinfo.getAddr(), hostinfo.getPort(), Optional.empty(), fn);
    }

    public List<String> getAvailableHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> e.getValue().getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.REACHABLE))
                .map(Map.Entry::getKey).toList());
    }

    public List<String> getSeenHosts() {
        return _transientPeersState.runReadLocked(d -> d.getStates().entrySet().stream()
                .filter(e -> !e.getValue().getState().equals(TransientPeersStateData.TransientPeerState.ConnectionState.NOT_SEEN))
                .map(Map.Entry::getKey).toList());
    }

}
