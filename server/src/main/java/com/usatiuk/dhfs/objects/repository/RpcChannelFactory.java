package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.repository.peertrust.PeerTrustManager;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import io.grpc.netty.NettyChannelBuilder;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import javax.net.ssl.KeyManagerFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

//FIXME: Leaks!
@ApplicationScoped
public class RpcChannelFactory {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;
    @Inject
    PeerTrustManager peerTrustManager;
    private ConcurrentMap<SecureChannelKey, ManagedChannel> _secureChannelCache = new ConcurrentHashMap<>();

    void shutdown(@Observes @Priority(100000) ShutdownEvent event) {
        for (var c : _secureChannelCache.values()) c.shutdownNow();
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

    ManagedChannel getSecureChannel(String host, String address, int port) {
        var key = new SecureChannelKey(host, address, port);
        return _secureChannelCache.computeIfAbsent(key, (k) -> {
            return NettyChannelBuilder.forAddress(address, port, getChannelCredentials()).overrideAuthority(host).idleTimeout(10, TimeUnit.SECONDS).build();
        });
    }

    public void dropCache() {
        var oldS = _secureChannelCache;
        _secureChannelCache = new ConcurrentHashMap<>();
        oldS.values().forEach(ManagedChannel::shutdown);
    }

    private record SecureChannelKey(String host, String address, int port) {
    }

    private record InsecureChannelKey(String address, int port) {
    }
}
