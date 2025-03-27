package com.usatiuk.dhfs.repository.peertrust;


import com.usatiuk.dhfs.repository.PersistentPeerDataService;
import io.quarkus.vertx.http.HttpServerOptionsCustomizer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.TrustOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.net.ssl.KeyManagerFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;

@ApplicationScoped
public class PeerTrustServerCustomizer implements HttpServerOptionsCustomizer {

    @Inject
    PeerTrustManager peerTrustManager;

    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Override
    public void customizeHttpsServer(HttpServerOptions options) {
        try {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);

            ks.setKeyEntry("sslkey",
                    persistentPeerDataService.getSelfKeypair().getPrivate(), null,
                    new Certificate[]{persistentPeerDataService.getSelfCertificate()});

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(ks, null);

            options.setKeyCertOptions(KeyCertOptions.wrap(keyManagerFactory));
            options.setTrustOptions(TrustOptions.wrap(peerTrustManager));
        } catch (Exception e) {
            throw new RuntimeException("Error configuring https: ", e);
        }
    }
}
