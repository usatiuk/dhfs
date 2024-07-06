package com.usatiuk.dhfs.storage.objects.repository.distributed.peertrust;

import com.usatiuk.dhfs.storage.objects.repository.distributed.peersync.PersistentPeerInfo;
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

@ApplicationScoped
public class PeerTrustManager implements X509TrustManager {
    private X509TrustManager trustManager;

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        trustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        trustManager.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManager.getAcceptedIssuers();
    }

    public void reloadTrustManagerHosts(Collection<PersistentPeerInfo> hosts) {
        try {
            Log.info("Trying to reload trust manager: " + hosts.size() + " known hosts");
            reloadTrustManager(hosts.stream().map(hostInfo ->
                    Pair.of(hostInfo.getUuid().toString(), hostInfo.getCertificate())).toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void reloadTrustManager(Collection<Pair<String, X509Certificate>> certs) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
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
                trustManager = (X509TrustManager) tm;
                return;
            }
        }

        throw new NoSuchAlgorithmException("No X509TrustManager in TrustManagerFactory");
    }

}