package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.repository.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.objects.repository.peerdiscovery.PeerAddress;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
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

    public <R> R withObjSyncClient(PeerId target, ObjectSyncClientFunction<R> fn) {
        var hostinfo = peerManager.getAddress(target);

        if (hostinfo == null)
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Not known to be reachable: " + target));

        return withObjSyncClient(target, hostinfo, syncTimeout, fn);
    }

    public <R> R withObjSyncClient(PeerId host, PeerAddress address, long timeout, ObjectSyncClientFunction<R> fn) {
        return switch (address) {
            case IpPeerAddress ipPeerAddress ->
                    withObjSyncClient(host, ipPeerAddress.address(), ipPeerAddress.securePort(), timeout, fn);
            default -> throw new IllegalStateException("Unexpected value: " + address);
        };
    }

    public <R> R withObjSyncClient(PeerId host, InetAddress addr, int port, long timeout, ObjectSyncClientFunction<R> fn) {
        var key = new ObjSyncStubKey(host, addr, port);
        var stub = _objSyncCache.computeIfAbsent(key, (k) -> {
            var channel = rpcChannelFactory.getSecureChannel(host, addr.getHostAddress(), port);
            return DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                    .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                    .withMaxInboundMessageSize(Integer.MAX_VALUE);
        });
        return fn.apply(stub.withDeadlineAfter(timeout, TimeUnit.SECONDS));
    }

    public void dropCache() {
        _objSyncCache = new ConcurrentHashMap<>();
    }

    @FunctionalInterface
    public interface ObjectSyncClientFunction<R> {
        R apply(DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    private record ObjSyncStubKey(PeerId id, InetAddress addr, int port) {
    }

}
