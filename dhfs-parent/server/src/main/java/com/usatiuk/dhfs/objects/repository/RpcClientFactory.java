package com.usatiuk.dhfs.objects.repository;

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

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RpcChannelFactory rpcChannelFactory;
    // FIXME: Leaks!
    private ConcurrentMap<ObjSyncStubKey, DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub> _objSyncCache = new ConcurrentHashMap<>();

    public <R> R withObjSyncClient(Collection<UUID> targets, ObjectSyncClientFunction<R> fn) {
        var shuffledList = new ArrayList<>(targets);
        Collections.shuffle(shuffledList);
        for (UUID target : shuffledList) {
            try {
                return withObjSyncClient(target, fn);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Status.UNAVAILABLE.getCode()))
                    Log.trace("Host " + target + " is unreachable: " + e.getMessage());
                else
                    Log.warn("When calling " + target + " " + e.getMessage());
            } catch (Exception e) {
                Log.warn("When calling " + target + " " + e.getMessage());
            }
        }
        throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("No reachable targets!"));
    }

    public <R> R withObjSyncClient(UUID target, ObjectSyncClientFunction<R> fn) {
        var hostinfo = remoteHostManager.getTransientState(target);
        boolean reachable = remoteHostManager.isReachable(target);

        if (hostinfo.getAddr() == null)
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Address for " + target + " not yet known"));

        if (!reachable)
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Not known to be reachable: " + target));

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

    public void dropCache() {
        rpcChannelFactory.dropCache();
        _objSyncCache = new ConcurrentHashMap<>();
    }

    @FunctionalInterface
    public interface ObjectSyncClientFunction<R> {
        R apply(DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    private record ObjSyncStubKey(String host, String address, int port) {
    }

}
