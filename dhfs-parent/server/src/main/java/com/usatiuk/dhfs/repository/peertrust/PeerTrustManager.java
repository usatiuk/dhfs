package com.usatiuk.dhfs.repository.peertrust;

import com.usatiuk.dhfs.repository.peersync.PeerInfo;
import com.usatiuk.dhfs.repository.peersync.PeerInfoService;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class PeerTrustManager implements X509TrustManager {
    private final AtomicReference<X509TrustManager> trustManager = new AtomicReference<>();
    @Inject
    PeerInfoService peerInfoService;

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        trustManager.get().checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        trustManager.get().checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManager.get().getAcceptedIssuers();
    }

    public synchronized void reloadTrustManagerHosts(Collection<PeerInfo> hosts) {
        try {
            Log.info("Trying to reload trust manager: " + hosts.size() + " known hosts");
            reloadTrustManager(hosts.stream().map(hostInfo ->
                    Pair.of(hostInfo.id().toString(), hostInfo.parsedCert())).toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void reloadTrustManager(Collection<Pair<String, X509Certificate>> certs) throws Exception {
        KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
        ts.load(null, null);

        for (var cert : certs) {
            ts.setCertificateEntry(cert.getLeft(), cert.getRight());
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        TrustManager[] tms = tmf.getTrustManagers();
        for (var tm : tms) {
            if (tm instanceof X509TrustManager) {
                trustManager.set((X509TrustManager) tm);
                return;
            }
        }

        throw new NoSuchAlgorithmException("No X509TrustManager in TrustManagerFactory");
    }

}