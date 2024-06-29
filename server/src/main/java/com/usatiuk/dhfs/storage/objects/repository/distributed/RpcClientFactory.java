package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.DhfsObjectSyncGrpcGrpc;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.DhfsObjectPeerSyncGrpcGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// TODO: Dedup this
@ApplicationScoped
public class RpcClientFactory {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @ConfigProperty(name = "dhfs.objects.distributed.sync.timeout")
    long syncTimeout;

    @ConfigProperty(name = "dhfs.objects.distributed.peersync.timeout")
    long peerSyncTimeout;

    @Inject
    RemoteHostManager remoteHostManager;

    @FunctionalInterface
    public interface ObjectSyncClientFunction<R> {
        R apply(DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    public <R> R withObjSyncClient(Collection<UUID> targets, ObjectSyncClientFunction<R> fn) {
        var shuffledList = new ArrayList<>(targets);
        Collections.shuffle(shuffledList);
        for (UUID target : shuffledList) {
            var hostinfo = remoteHostManager.getTransientState(target);

            boolean shouldTry = remoteHostManager.isReachable(target)
                    && hostinfo.getAddr() != null;

            if (!shouldTry) continue;

            try {
                return withObjSyncClient(hostinfo.getAddr(), hostinfo.getPort(), syncTimeout, fn);
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
        if (hostinfo.getAddr() == null)
            throw new IllegalStateException("Address for " + target + " not yet known");
        return withObjSyncClient(hostinfo.getAddr(), hostinfo.getPort(), syncTimeout, fn);
    }

    public <R> R withObjSyncClient(String addr, int port, long timeout, ObjectSyncClientFunction<R> fn) {
        var channel = NettyChannelBuilder.forAddress(addr, port).negotiationType(NegotiationType.PLAINTEXT)
                .usePlaintext().build();
        var client = DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                .withMaxInboundMessageSize(Integer.MAX_VALUE)
                .withDeadlineAfter(timeout, TimeUnit.SECONDS);
        try {
            return fn.apply(client);
        } finally {
            channel.shutdownNow();
        }
    }

    @FunctionalInterface
    public interface PeerSyncClientFunction<R> {
        R apply(DhfsObjectPeerSyncGrpcGrpc.DhfsObjectPeerSyncGrpcBlockingStub client);
    }

    public <R> R withPeerSyncClient(UUID target, PeerSyncClientFunction<R> fn) {
        var hostinfo = remoteHostManager.getTransientState(target);
        if (hostinfo.getAddr() == null)
            throw new IllegalStateException("Address for " + target + " not yet known");
        return withPeerSyncClient(hostinfo.getAddr(), hostinfo.getPort(), peerSyncTimeout, fn);
    }

    public <R> R withPeerSyncClient(String addr, int port, long timeout, PeerSyncClientFunction<R> fn) {
        var channel = NettyChannelBuilder.forAddress(addr, port).negotiationType(NegotiationType.PLAINTEXT)
                .usePlaintext().build();
        var client = DhfsObjectPeerSyncGrpcGrpc.newBlockingStub(channel)
                .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                .withMaxInboundMessageSize(Integer.MAX_VALUE)
                .withDeadlineAfter(timeout, TimeUnit.SECONDS);
        try {
            return fn.apply(client);
        } finally {
            channel.shutdownNow();
        }
    }


}
