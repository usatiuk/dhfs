package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.ShutdownChecker;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.peersync.PeerInfoService;
import com.usatiuk.dhfs.repository.peertrust.PeerTrustManager;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.HashTreePSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

@ApplicationScoped
public class PersistentPeerDataService {
    @Inject
    PeerTrustManager peerTrustManager;
    @Inject
    ExecutorService executorService;
    @Inject
    RpcClientFactory rpcClientFactory;
    @Inject
    ShutdownChecker shutdownChecker;
    @Inject
    TransactionManager jObjectTxManager;
    @Inject
    Transaction curTx;
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    TransactionManager txm;

    @ConfigProperty(name = "dhfs.peerdiscovery.preset-uuid")
    Optional<String> presetUuid;
    @ConfigProperty(name = "dhfs.objects.persistence.stuff.root")
    String stuffRoot;

    private PeerId _selfUuid;
    private X509Certificate _selfCertificate;
    private KeyPair _selfKeyPair;

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        jObjectTxManager.run(() -> {
            var selfData = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (selfData != null) {
                _selfUuid = selfData.selfUuid();
                _selfCertificate = selfData.selfCertificate();
                _selfKeyPair = selfData.selfKeyPair();
                return;
            } else {
                try {
                    _selfUuid = presetUuid.map(PeerId::of).orElseGet(() -> PeerId.of(UUID.randomUUID().toString()));
                    Log.info("Generating a key pair, please wait");
                    _selfKeyPair = CertificateTools.generateKeyPair();
                    _selfCertificate = CertificateTools.generateCertificate(_selfKeyPair, _selfUuid.toString());

                    curTx.put(new PersistentRemoteHostsData(_selfUuid, _selfCertificate, _selfKeyPair, HashTreePSet.empty()));
                    peerInfoService.putPeer(_selfUuid, _selfCertificate.getEncoded());
                } catch (CertificateEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        peerTrustManager.reloadTrustManagerHosts(peerInfoService.getPeers());
        Log.info("Self uuid is: " + _selfUuid.toString());
        new File(stuffRoot).mkdirs();
        Files.write(Path.of(stuffRoot, "self_uuid"), _selfUuid.id().toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

//    private void pushPeerUpdates() {
//        pushPeerUpdates(null);
//    }

//    private void pushPeerUpdates(@Nullable JObject<?> obj) {
//        if (obj != null)
//            Log.info("Scheduling certificate update after " + obj.getMeta().getName() + " was updated");
//        executorService.submit(() -> {
//            updateCerts();
//            invalidationQueueService.pushInvalidationToAll(PeerDirectory.PeerDirectoryObjName);
//            for (var p : peerDirectory.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.getPeers().stream().toList()))
//                invalidationQueueService.pushInvalidationToAll(PersistentPeerInfo.getNameFromUuid(p));
//        });
//    }

    public PeerId getSelfUuid() {
        return _selfUuid;
    }

    public void updateCerts() {
        peerTrustManager.reloadTrustManagerHosts(txm.run(() -> peerInfoService.getPeers().stream().toList())); //FIXME:
    }

    public KeyPair getSelfKeypair() {
        return _selfKeyPair;
    }

    public X509Certificate getSelfCertificate() {
        return _selfCertificate;
    }

    // Returns true if host's initial sync wasn't done before, and marks it as done
    public boolean markInitialSyncDone(PeerId peerId) {
        return txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            boolean exists = data.initialSyncDone().contains(peerId);
            if (exists) {
                Log.tracev("Already marked sync state for {0}", peerId);
                return false;
            }
            curTx.put(data.withInitialSyncDone(data.initialSyncDone().plus(peerId)));
            Log.infov("Did mark sync state for {0}", peerId);
            return true;
        });
    }

    // Returns true if it was marked as done before, and resets it
    public boolean resetInitialSyncDone(PeerId peerId) {
        return txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            boolean exists = data.initialSyncDone().contains(peerId);
            if (!exists) {
                Log.infov("Already reset sync state for {0}", peerId);
                return false;
            }
            curTx.put(data.withInitialSyncDone(data.initialSyncDone().minus(peerId)));
            Log.infov("Did reset sync state for {0}", peerId);
            return true;
        });
    }

    public boolean isInitialSyncDone(PeerId peerId) {
        return txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            return data.initialSyncDone().contains(peerId);
        });
    }
}
