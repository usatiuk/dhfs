package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.DhfsObjectSyncGrpcGrpc;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.DhfsObjectPeerSyncGrpcGrpc;
import com.usatiuk.dhfs.storage.objects.repository.distributed.peertrust.PeerTrustManager;
import io.grpc.ChannelCredentials;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.net.ssl.KeyManagerFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;

// TODO: Dedup this
@ApplicationScoped
public class RpcClientFactory {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Inject
    PeerTrustManager peerTrustManager;

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

            boolean reachable = remoteHostManager.isReachable(target);
            var addr = hostinfo.getAddr();
            boolean shouldTry = reachable && addr != null;

            if (!shouldTry) {
                Log.trace("Not trying " + target + ": " + "addr=" + Objects.toString(addr) + " reachable=" + reachable);
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

    private ChannelCredentials getChannelCredentials() {
        try {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);

            ks.setKeyEntry("clientkey", persistentRemoteHostsService.getSelfKeypair().getPrivate(), null, new Certificate[]{persistentRemoteHostsService.getSelfCertificate()});

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(ks, null);

            ChannelCredentials creds = TlsChannelCredentials.newBuilder().trustManager(peerTrustManager).keyManager(keyManagerFactory.getKeyManagers()).build();
            return creds;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <R> R withObjSyncClient(String host, String addr, int port, long timeout, ObjectSyncClientFunction<R> fn) {
        var creds = getChannelCredentials();
        var channel = NettyChannelBuilder.forAddress(addr, port, creds).overrideAuthority(host).build();
        var client = DhfsObjectSyncGrpcGrpc.newBlockingStub(channel).withMaxOutboundMessageSize(Integer.MAX_VALUE).withMaxInboundMessageSize(Integer.MAX_VALUE).withDeadlineAfter(timeout, TimeUnit.SECONDS);
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
        if (hostinfo.getAddr() == null) throw new IllegalStateException("Address for " + target + " not yet known");
        return withPeerSyncClient(hostinfo.getAddr(), hostinfo.getPort(), peerSyncTimeout, fn);
    }

    public <R> R withPeerSyncClient(String addr, int port, long timeout, PeerSyncClientFunction<R> fn) {
        var channel = NettyChannelBuilder.forAddress(addr, port).negotiationType(NegotiationType.PLAINTEXT).usePlaintext().build();
        var client = DhfsObjectPeerSyncGrpcGrpc.newBlockingStub(channel).withMaxOutboundMessageSize(Integer.MAX_VALUE).withMaxInboundMessageSize(Integer.MAX_VALUE).withDeadlineAfter(timeout, TimeUnit.SECONDS);
        try {
            return fn.apply(client);
        } finally {
            channel.shutdownNow();
        }
    }


}
