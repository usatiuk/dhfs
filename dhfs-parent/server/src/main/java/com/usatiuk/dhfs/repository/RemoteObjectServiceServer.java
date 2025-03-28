package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.repository.peerdiscovery.PeerAddressType;
import com.usatiuk.dhfs.repository.peerdiscovery.PeerDiscoveryDirectory;
import io.quarkus.grpc.GrpcService;
import io.quarkus.grpc.RegisterInterceptor;
import io.quarkus.logging.Log;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.function.Function;

import static com.usatiuk.dhfs.repository.ProxyConstants.*;

// Note: RunOnVirtualThread hangs somehow

@GrpcService
@RolesAllowed("cluster-member")
@RegisterInterceptor(ProxyServerInterceptor.class)
public class RemoteObjectServiceServer implements DhfsObjectSyncGrpc {
    @Inject
    SecurityIdentity identity;
    @Inject
    RemoteObjectServiceServerImpl remoteObjectServiceServerImpl;
    @Inject
    RpcClientFactory rpcClientFactory;
    @Inject
    PeerManager peerManager;
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;

    private PeerId getIdentity() {
        if (PROXY_FROM_HEADER_KEY_CTX.get() != null) {
            return PeerId.of(PROXY_FROM_HEADER_KEY_CTX.get());
        }

        return PeerId.of(identity.getPrincipal().getName().substring(3));
    }

    private PeerId getProxyTarget() {
        if (PROXY_TO_HEADER_KEY_CTX.get() != null) {
            return PeerId.of(PROXY_TO_HEADER_KEY_CTX.get());
        }
        return null;
    }

    private <T> Optional<Uni<T>> tryProxy(Function<DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub, T> fn) {
        var proxyTarget = getProxyTarget();
        if (proxyTarget != null) {
            var fromAddr = PROXY_TO_FROM_ADDR_KEY_CTX.get();
            if (fromAddr instanceof InetSocketAddress inetAddr) {
                peerDiscoveryDirectory.notifyAddr(
                        new IpPeerAddress(
                                PeerId.of(identity.getPrincipal().getName().substring(3)),
                                PeerAddressType.WAN,
                                inetAddr.getAddress(),
                                -1,
                                inetAddr.getPort()
                        )
                );
            } else {
                Log.warnv("Expected InetSocketAddress but got {0}", fromAddr);
            }

            return Optional.of(Uni.createFrom().item(rpcClientFactory.<T>withObjSyncClient(
                    proxyTarget,
                    getIdentity(),
                    null,
                    (peer, client) -> {
                        if (!peer.equals(proxyTarget)) {
                            throw new IllegalStateException("Expected " + proxyTarget + " but got " + peer + " when proxying");
                        }
                        Log.tracev("Proxying to {0}", peer);
                        return fn.apply(client);
                    }
            )));
        }
        return Optional.empty();
    }

    @Override
    @Blocking
    public Uni<GetObjectReply> getObject(GetObjectRequest request) {
        return tryProxy(client -> client.getObject(request)).orElseGet(() -> remoteObjectServiceServerImpl.getObject(getIdentity(), request));
    }

    @Override
    @Blocking
    public Uni<CanDeleteReply> canDelete(CanDeleteRequest request) {
        return tryProxy(client -> client.canDelete(request)).orElseGet(() -> remoteObjectServiceServerImpl.canDelete(getIdentity(), request));
    }

    @Override
    @Blocking
    public Uni<OpPushReply> opPush(OpPushRequest request) {
        return tryProxy(client -> client.opPush(request)).orElseGet(() -> remoteObjectServiceServerImpl.opPush(getIdentity(), request));
    }

    @Override
    @Blocking
    public Uni<PingReply> ping(PingRequest request) {
        return tryProxy(client -> client.ping(request)).orElseGet(() -> remoteObjectServiceServerImpl.ping(getIdentity(), request));
    }

    @Override
    @Blocking
    public Uni<ProxyAvailableReply> proxyAvailableGet(ProxyAvailableRequest request) {
        var got = peerManager.getDirectAvailableHosts();
        var builder = ProxyAvailableReply.newBuilder();
        for (var host : got) {
            builder.addAvailableTargetsBuilder().setUuid(host.toString());
        }
        return Uni.createFrom().item(builder.build());
    }
}
