package com.usatiuk.dhfs.rpc;

import com.usatiuk.dhfs.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.peerdiscovery.PeerAddress;
import com.usatiuk.dhfs.peersync.PeerDisconnectedEventListener;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.ReachablePeerManager;
import com.usatiuk.dhfs.repository.DhfsObjectSyncGrpcGrpc;
import io.grpc.ManagedChannel;
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

/**
 * Factory for creating gRPC clients for object synchronization and other RPC calls.
 */
@ApplicationScoped
public class RpcClientFactory implements PeerDisconnectedEventListener {
    @ConfigProperty(name = "dhfs.objects.sync.timeout")
    long syncTimeout;

    @Inject
    ReachablePeerManager reachablePeerManager;

    @Inject
    RpcChannelFactory rpcChannelFactory;

    private ConcurrentMap<ObjSyncStubKey, DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub> _objSyncCache = new ConcurrentHashMap<>();

    /**
     * Try calling a given function on given peers in random order.
     *
     * @param targets the list of targets to call
     * @param fn      the function to call
     * @param <R>     the return type of the function
     * @return the result of the function call
     */
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

    /**
     * Try calling a given function on a given target.
     *
     * @param target the target to call
     * @param fn     the function to call
     * @param <R>    the return type of the function
     * @return the result of the function call
     */
    public <R> R withObjSyncClient(PeerId target, ObjectSyncClientFunction<R> fn) {
        var hostinfo = reachablePeerManager.getAddress(target);

        if (hostinfo == null)
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Not known to be reachable: " + target));

        return withObjSyncClient(target, hostinfo, syncTimeout, fn);
    }

    /**
     * Try calling a given function on a given target with a specified timeout.
     *
     * @param host    the host to call
     * @param address the address of the host
     * @param timeout the timeout for the call
     * @param fn      the function to call
     * @param <R>     the return type of the function
     * @return the result of the function call
     */
    public <R> R withObjSyncClient(PeerId host, PeerAddress address, long timeout, ObjectSyncClientFunction<R> fn) {
        return switch (address) {
            case IpPeerAddress ipPeerAddress ->
                    withObjSyncClient(host, ipPeerAddress.address(), ipPeerAddress.securePort(), timeout, fn);
            default -> throw new IllegalStateException("Unexpected value: " + address);
        };
    }

    /**
     * Try calling a given function on a given target with a specified timeout.
     *
     * @param host    the host to call
     * @param addr    the address of the host
     * @param port    the port of the host
     * @param timeout the timeout for the call
     * @param fn      the function to call
     * @param <R>     the return type of the function
     * @return the result of the function call
     */
    public <R> R withObjSyncClient(PeerId host, InetAddress addr, int port, long timeout, ObjectSyncClientFunction<R> fn) {
        var key = new ObjSyncStubKey(host, addr, port);
        var stub = _objSyncCache.computeIfAbsent(key, (k) -> {
            var channel = rpcChannelFactory.getSecureChannel(host, addr.getHostAddress(), port);
            return DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                    .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                    .withMaxInboundMessageSize(Integer.MAX_VALUE);
        });
        return fn.apply(host, stub.withDeadlineAfter(timeout, TimeUnit.SECONDS));
    }

    @Override
    public void handlePeerDisconnected(PeerId peerId) {
        ArrayList<ObjSyncStubKey> toRemove = new ArrayList<>();
        for (var objSyncStubKey : _objSyncCache.keySet()) {
            if (objSyncStubKey.id().equals(peerId)) {
                toRemove.add(objSyncStubKey);
            }
        }
        for (var objSyncStubKey : toRemove) {
            var stub = _objSyncCache.remove(objSyncStubKey);
            if (stub != null) {
                ((ManagedChannel) stub.getChannel()).shutdown();
            }
        }
    }

    @FunctionalInterface
    public interface ObjectSyncClientFunction<R> {
        R apply(PeerId peer, DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    private record ObjSyncStubKey(PeerId id, InetAddress addr, int port) {
    }

}
