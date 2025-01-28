package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.ShutdownChecker;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfoService;
import com.usatiuk.dhfs.objects.repository.peertrust.PeerTrustManager;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.security.KeyPair;
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

    @ConfigProperty(name = "dhfs.peerdiscovery.preset-uuid")
    Optional<String> presetUuid;

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
                _selfUuid = presetUuid.map(s -> PeerId.of(UUID.fromString(s))).orElseGet(() -> PeerId.of(UUID.randomUUID()));
                try {
                    Log.info("Generating a key pair, please wait");
                    _selfKeyPair = CertificateTools.generateKeyPair();
                    _selfCertificate = CertificateTools.generateCertificate(_selfKeyPair, _selfUuid.toString());

                    curTx.put(new PersistentRemoteHostsData(_selfUuid, 0, _selfCertificate, _selfKeyPair));
                    peerInfoService.putPeer(_selfUuid, _selfCertificate.getEncoded());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        peerTrustManager.reloadTrustManagerHosts(peerInfoService.getPeers());
        Log.info("Self uuid is: " + _selfUuid.toString());
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

    public long getUniqueId() {
        return jObjectTxManager.run(() -> {
            var curData = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            curTx.put(curData.withSelfCounter(curData.selfCounter() + 1));
            return curData.selfCounter();
        });
    }

//    private void updateCerts() {
//        try {
//            peerDirectory.get().runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
//                peerTrustManager.reloadTrustManagerHosts(getHostsNoNulls());
//                // Fixme:? I don't think it should be needed with custom trust store
//                // but it doesn't work?
//                rpcClientFactory.dropCache();
//                return null;
//            });
//        } catch (Exception ex) {
//            Log.warn("Error when refreshing certificates, will retry: " + ex.getMessage());
//            pushPeerUpdates();
//        }
//    }

    public KeyPair getSelfKeypair() {
        return _selfKeyPair;
    }

    public X509Certificate getSelfCertificate() {
        return _selfCertificate;
    }

//    // Returns true if host's initial sync wasn't done before, and marks it as done
//    public boolean markInitialOpSyncDone(UUID connectedHost) {
//        return jObjectTxManager.executeTx(() -> {
//            peerDirectoryLocal.get().rwLock();
//            try {
//                peerDirectoryLocal.get().local();
//                boolean contained = peerDirectoryLocal.get().getData().getInitialOpSyncDone().contains(connectedHost);
//
//                if (!contained)
//                    peerDirectoryLocal.get().local().mutate(new JMutator<PeerDirectoryLocal>() {
//                        @Override
//                        public boolean mutate(PeerDirectoryLocal object) {
//                            object.getInitialOpSyncDone().add(connectedHost);
//                            return true;
//                        }
//
//                        @Override
//                        public void revert(PeerDirectoryLocal object) {
//                            object.getInitialOpSyncDone().remove(connectedHost);
//                        }
//                    });
//                return !contained;
//            } finally {
//                peerDirectoryLocal.get().rwUnlock();
//            }
//        });
//    }
//
//    public boolean markInitialObjSyncDone(UUID connectedHost) {
//        return jObjectTxManager.executeTx(() -> {
//            peerDirectoryLocal.get().rwLock();
//            try {
//                peerDirectoryLocal.get().local();
//                boolean contained = peerDirectoryLocal.get().getData().getInitialObjSyncDone().contains(connectedHost);
//
//                if (!contained)
//                    peerDirectoryLocal.get().local().mutate(new JMutator<PeerDirectoryLocal>() {
//                        @Override
//                        public boolean mutate(PeerDirectoryLocal object) {
//                            object.getInitialObjSyncDone().add(connectedHost);
//                            return true;
//                        }
//
//                        @Override
//                        public void revert(PeerDirectoryLocal object) {
//                            object.getInitialObjSyncDone().remove(connectedHost);
//                        }
//                    });
//                return !contained;
//            } finally {
//                peerDirectoryLocal.get().rwUnlock();
//            }
//        });
//    }

}
