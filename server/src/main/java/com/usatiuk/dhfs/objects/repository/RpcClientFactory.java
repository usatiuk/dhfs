package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.repository.peersync.DhfsObjectPeerSyncGrpcGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

// TODO: Dedup this
@ApplicationScoped
public class RpcClientFactory {
    @ConfigProperty(name = "dhfs.objects.sync.timeout")
    long syncTimeout;

    @ConfigProperty(name = "dhfs.objects.peersync.timeout")
    long peerSyncTimeout;

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RpcChannelFactory rpcChannelFactory;

    private record ObjSyncStubKey(String host, String address, int port) {
    }

    private record PeerSyncStubKey(String address, int port) {
    }

    // FIXME: Leaks!
    private final ConcurrentMap<ObjSyncStubKey, DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub> _objSyncCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<PeerSyncStubKey, DhfsObjectPeerSyncGrpcGrpc.DhfsObjectPeerSyncGrpcBlockingStub> _peerSyncCache = new ConcurrentHashMap<>();


    @FunctionalInterface
    public interface ObjectSyncClientFunction<R> {
        R apply(DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    public <R> R withObjSyncClient(Collection<UUID> targets, ObjectSyncClientFunction<R> fn) {
        var shuffledList = new ArrayList<>(targets);
        Collections.shuffle(shuffledList);
        for (UUID target : shuffledList) {
            var hostinfo = remoteHostManager.getTransientState(target);

            boolean reachable = remoteHostManager.isReachable(target);
            var addr = hostinfo.getAddr();
            boolean shouldTry = reachable && addr != null;

            if (!shouldTry) {
                Log.trace("Not trying " + target + ": " + "addr=" + addr + " reachable=" + reachable);
                continue;
            }

            try {
                return withObjSyncClient(target.toString(), hostinfo.getAddr(), hostinfo.getSecurePort(), syncTimeout, fn);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
                    Log.info("Host " + target + " is unreachable: " + e.getMessage());
                    remoteHostManager.handleConnectionError(target);
                } else {
                    Log.error("When calling " + target, e);
                    continue;
                }
            } catch (Exception e) {
                Log.error("When calling " + target, e);
                continue;
            }
        }
        throw new IllegalStateException("No reachable targets!");
    }


    public <R> R withObjSyncClient(UUID target, ObjectSyncClientFunction<R> fn) {
        var hostinfo = remoteHostManager.getTransientState(target);
        if (hostinfo.getAddr() == null) throw new IllegalStateException("Address for " + target + " not yet known");
        return withObjSyncClient(target.toString(), hostinfo.getAddr(), hostinfo.getSecurePort(), syncTimeout, fn);
    }


    public <R> R withObjSyncClient(String host, String addr, int port, long timeout, ObjectSyncClientFunction<R> fn) {
        var key = new ObjSyncStubKey(host, addr, port);
        var stub = _objSyncCache.computeIfAbsent(key, (k) -> {
            var channel = rpcChannelFactory.getSecureChannel(host, addr, port);
            return DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                    .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                    .withMaxInboundMessageSize(Integer.MAX_VALUE);

        });
        return fn.apply(stub.withDeadlineAfter(timeout, TimeUnit.SECONDS));
    }

    @FunctionalInterface
    public interface PeerSyncClientFunction<R> {
        R apply(DhfsObjectPeerSyncGrpcGrpc.DhfsObjectPeerSyncGrpcBlockingStub client);
    }

    public <R> R withPeerSyncClient(UUID target, PeerSyncClientFunction<R> fn) {
        var hostinfo = remoteHostManager.getTransientState(target);
        if (hostinfo.getAddr() == null) throw new IllegalStateException("Address for " + target + " not yet known");
        return withPeerSyncClient(hostinfo.getAddr(), hostinfo.getPort(), peerSyncTimeout, fn);
    }

    public <R> R withPeerSyncClient(String addr, int port, long timeout, PeerSyncClientFunction<R> fn) {
        var key = new PeerSyncStubKey(addr, port);
        var stub = _peerSyncCache.computeIfAbsent(key, (k) -> {
            var channel = rpcChannelFactory.getInsecureChannel(addr, port);
            return DhfsObjectPeerSyncGrpcGrpc.newBlockingStub(channel)
                    .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                    .withMaxInboundMessageSize(Integer.MAX_VALUE);
        });
        return fn.apply(stub.withDeadlineAfter(timeout, TimeUnit.SECONDS));
    }
}
