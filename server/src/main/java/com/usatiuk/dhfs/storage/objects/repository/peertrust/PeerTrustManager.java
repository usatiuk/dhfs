package com.usatiuk.dhfs.storage.objects.repository.peertrust;

import com.usatiuk.dhfs.storage.objects.repository.peersync.PersistentPeerInfo;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.tuple.Pair;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class PeerTrustManager implements X509TrustManager {
    private final AtomicReference<X509TrustManager> trustManager = new AtomicReference<>();

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

    public synchronized void reloadTrustManagerHosts(Collection<PersistentPeerInfo> hosts) {
        try {
            Log.info("Trying to reload trust manager: " + hosts.size() + " known hosts");
            reloadTrustManager(hosts.stream().map(hostInfo ->
                    Pair.of(hostInfo.getUuid().toString(), hostInfo.getCertificate())).toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void reloadTrustManager(Collection<Pair<String, X509Certificate>> certs) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
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