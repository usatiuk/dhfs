package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.peertrust.PeerTrustManager;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import io.grpc.netty.NettyChannelBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.net.ssl.KeyManagerFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class RpcChannelFactory {
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    PeerTrustManager peerTrustManager;

    private ChannelCredentials getChannelCredentials() {
        try {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);

            ks.setKeyEntry("clientkey", persistentPeerDataService.getSelfKeypair().getPrivate(), null, new Certificate[]{persistentPeerDataService.getSelfCertificate()});

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(ks, null);

            ChannelCredentials creds = TlsChannelCredentials.newBuilder().trustManager(peerTrustManager).keyManager(keyManagerFactory.getKeyManagers()).build();
            return creds;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    ManagedChannel getSecureChannel(PeerId host, String address, int port) {
        return NettyChannelBuilder.forAddress(address, port, getChannelCredentials()).overrideAuthority(host.toString()).idleTimeout(10, TimeUnit.SECONDS).build();
    }
}
