package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.repository.peerdiscovery.PeerAddress;
import com.usatiuk.dhfs.repository.peerdiscovery.ProxyPeerAddress;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClientUtils;
import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.usatiuk.dhfs.repository.ProxyConstants.PROXY_FROM_HEADER_KEY;
import static com.usatiuk.dhfs.repository.ProxyConstants.PROXY_TO_HEADER_KEY;

// TODO: Dedup this
@ApplicationScoped
public class RpcClientFactory {
    @ConfigProperty(name = "dhfs.objects.sync.timeout")
    long syncTimeout;

    @Inject
    PeerManager peerManager;

    @Inject
    RpcChannelFactory rpcChannelFactory;

    // FIXME: Leaks!
    private ConcurrentMap<ObjSyncStubKey, DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub> _objSyncCache = new ConcurrentHashMap<>();

    public <R> R withObjSyncClient(Collection<PeerId> targets, ObjectSyncClientFunction<R> fn) {
        var shuffledList = new ArrayList<>(targets);
        Collections.shuffle(shuffledList);
        for (PeerId target : shuffledList) {
            try {
                return withObjSyncClient(target, fn);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Status.UNAVAILABLE.getCode()))
                    Log.debug("Host " + target + " is unreachable: " + e.getMessage());
                else
                    Log.warn("When calling " + target + " " + e.getMessage());
            } catch (Exception e) {
                Log.warn("When calling " + target + " " + e.getMessage());
            }
        }
        throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("No reachable targets!"));
    }

    public <R> R withObjSyncClient(PeerId target, PeerId proxyFrom, PeerId proxyTo, ObjectSyncClientFunction<R> fn) {
        var hostInfo = peerManager.getAddress(target);

        if (hostInfo == null)
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Not known to be reachable: " + target));

        return withObjSyncClient(target, proxyFrom, proxyTo, hostInfo, syncTimeout, fn);
    }

    public <R> R withObjSyncClient(PeerId host, PeerId proxyFrom, PeerId proxyTo, PeerAddress address, long timeout, ObjectSyncClientFunction<R> fn) {
        return switch (address) {
            case IpPeerAddress ipPeerAddress ->
                    withObjSyncClient(host, proxyFrom, proxyTo, ipPeerAddress.address(), ipPeerAddress.securePort(), timeout, fn);
            case ProxyPeerAddress pp -> withObjSyncClient(pp.proxyThrough(), null, host, fn); // TODO: Timeout
            default -> throw new IllegalStateException("Unexpected value: " + address);
        };
    }

    public <R> R withObjSyncClient(PeerId target, ObjectSyncClientFunction<R> fn) {
        return withObjSyncClient(target, null, null, fn);
    }

    public <R> R withObjSyncClient(PeerId host, PeerAddress address, long timeout, ObjectSyncClientFunction<R> fn) {
        return withObjSyncClient(host, null, null, address, timeout, fn);
    }

    public <R> R withObjSyncClient(PeerId host, @Nullable PeerId proxyFrom, @Nullable PeerId proxyTo, InetAddress addr, int port, long timeout, ObjectSyncClientFunction<R> fn) {
        var key = new ObjSyncStubKey(host, proxyFrom, proxyTo, addr, port);
        var stub = _objSyncCache.computeIfAbsent(key, (k) -> {
            var channel = rpcChannelFactory.getSecureChannel(host, addr.getHostAddress(), port);

            var client = DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                    .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                    .withMaxInboundMessageSize(Integer.MAX_VALUE);

            if (proxyFrom != null) {
                Metadata headers = new Metadata();
                headers.put(PROXY_FROM_HEADER_KEY, proxyFrom.toString());
                return GrpcClientUtils.attachHeaders(client, headers);
            } else if (proxyTo != null) {
                Metadata headers = new Metadata();
                headers.put(PROXY_TO_HEADER_KEY, proxyTo.toString());
                return GrpcClientUtils.attachHeaders(client, headers);
            } else {
                return client;
            }
        });
        return fn.apply(host, stub.withDeadlineAfter(timeout, TimeUnit.SECONDS));
    }

    public void dropCache() {
        _objSyncCache = new ConcurrentHashMap<>();
    }

    @FunctionalInterface
    public interface ObjectSyncClientFunction<R> {
        R apply(PeerId peer, DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    private record ObjSyncStubKey(PeerId id, @Nullable PeerId from, @Nullable PeerId to, InetAddress addr, int port) {
    }

}
