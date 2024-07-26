package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.JObjectResolver;
import com.usatiuk.dhfs.objects.repository.peersync.PeerDirectory;
import com.usatiuk.dhfs.objects.repository.peersync.PersistentPeerInfo;
import com.usatiuk.dhfs.objects.repository.peertrust.PeerTrustManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Nullable;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

@ApplicationScoped
public class PersistentRemoteHostsService {
    final String dataFileName = "hosts";
    @ConfigProperty(name = "dhfs.objects.root")
    String dataRoot;
    @Inject
    PeerTrustManager peerTrustManager;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    JObjectResolver jObjectResolver;
    @Inject
    ExecutorService executorService;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    RpcClientFactory rpcClientFactory;
    private PersistentRemoteHosts _persistentData = new PersistentRemoteHosts();

    private UUID _selfUuid;

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading hosts");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }
        _selfUuid = _persistentData.runReadLocked(PersistentRemoteHostsData::getSelfUuid);

        if (_persistentData.runReadLocked(d -> d.getSelfCertificate() == null)) {
            _persistentData.runWriteLocked(d -> {
                try {
                    Log.info("Generating a key pair, please wait");
                    d.setSelfKeyPair(CertificateTools.generateKeyPair());
                    d.setSelfCertificate(CertificateTools.generateCertificate(d.getSelfKeyPair(), _selfUuid.toString()));
                } catch (Exception e) {
                    throw new RuntimeException("Failed generating cert", e);
                }
                return null;
            });
            var newpd = new PeerDirectory();
            jObjectManager.put(new PersistentPeerInfo(_selfUuid, getSelfCertificate()), Optional.of(PeerDirectory.PeerDirectoryObjName));
            newpd.getPeers().add(_selfUuid);
            var dir = jObjectManager.put(newpd, Optional.empty());
        }

        jObjectResolver.registerWriteListener(PersistentPeerInfo.class, this::pushPeerUpdates);
        jObjectResolver.registerWriteListener(PeerDirectory.class, this::pushPeerUpdates);

        // FIXME: Warn on failed resolves?
        getPeerDirectory().runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
            peerTrustManager.reloadTrustManagerHosts(getHosts());
            return null;
        });

        Files.writeString(Paths.get(dataRoot, "self_uuid"), _selfUuid.toString());
        Log.info("Self uuid is: " + _selfUuid.toString());
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving hosts");
        Files.write(Paths.get(dataRoot).resolve(dataFileName), SerializationUtils.serialize(_persistentData));
        Log.info("Shutdown");
    }

    private JObject<PeerDirectory> getPeerDirectory() {
        var got = jObjectManager.get(PeerDirectory.PeerDirectoryObjName).orElseThrow(() -> new IllegalStateException("Peer directory not found"));
        got.runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
            if (d == null) throw new IllegalStateException("Could not resolve peer directory!");
            if (!(d instanceof PeerDirectory))
                throw new IllegalStateException("Peer directory is of wrong type!");
            return null;
        });
        return (JObject<PeerDirectory>) got;
    }

    private void pushPeerUpdates() {
        pushPeerUpdates(null);
    }

    private void pushPeerUpdates(@Nullable JObject<?> obj) {
        if (obj != null)
            Log.info("Scheduling certificate update after " + obj.getName() + " was updated");
        executorService.submit(() -> {
            updateCerts();
            invalidationQueueService.pushInvalidationToAll(PeerDirectory.PeerDirectoryObjName);
            for (var p : getPeerDirectory().runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.getPeers().stream().toList()))
                invalidationQueueService.pushInvalidationToAll(PersistentPeerInfo.getNameFromUuid(p));
        });
    }

    private JObject<PersistentPeerInfo> getPeer(UUID uuid) {
        var got = jObjectManager.get(PersistentPeerInfo.getNameFromUuid(uuid)).orElseThrow(() -> new IllegalStateException("Peer " + uuid + " not found"));
        got.runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
            if (d == null) throw new IllegalStateException("Could not resolve peer " + uuid);
            if (!(d instanceof PersistentPeerInfo))
                throw new IllegalStateException("Peer " + uuid + " is of wrong type!");
            return null;
        });
        return (JObject<PersistentPeerInfo>) got;
    }

    private List<PersistentPeerInfo> getPeersSnapshot() {
        return getPeerDirectory().runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY,
                (m, d) -> d.getPeers().stream().map(u -> {
                    try {
                        return getPeer(u).runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m2, d2) -> d2);
                    } catch (Exception e) {
                        Log.warn("Error making snapshot of peer " + u, e);
                        return null;
                    }
                }).filter(Objects::nonNull).toList());
    }

    public UUID getSelfUuid() {
        if (_selfUuid == null)
            throw new IllegalStateException();
        else return _selfUuid;
    }

    public String getUniqueId() {
        return _selfUuid.toString() + _persistentData.getData().getSelfCounter().addAndGet(1);
    }

    public PersistentPeerInfo getInfo(UUID name) {
        return getPeer(name).runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d);
    }

    public List<PersistentPeerInfo> getHosts() {
        return getPeersSnapshot().stream().filter(i -> !i.getUuid().equals(_selfUuid)).toList();
    }

    public List<UUID> getHostsUuid() {
        return getPeerDirectory().runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.getPeers().stream().filter(i -> !i.equals(_selfUuid)).toList());
    }

    public List<PersistentPeerInfo> getHostsNoNulls() {
        for (int i = 0; i < 5; i++) {
            try {
                return getPeerDirectory()
                        .runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY,
                                (m, d) -> d.getPeers().stream()
                                        .map(u -> getPeer(u).runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m2, d2) -> d2))
                                        .filter(e -> !e.getUuid().equals(_selfUuid)).toList());
            } catch (Exception e) {
                Log.warn("Error when making snapshot of hosts ", e);
                try {
                    Thread.sleep(i * 2);
                } catch (InterruptedException ignored) {
                }
            }
        }
        throw new StatusRuntimeException(Status.ABORTED.withDescription("Could not make a snapshot of peers in 5 tries!"));
    }

    public boolean addHost(PersistentPeerInfo persistentPeerInfo) {
        if (persistentPeerInfo.getUuid().equals(_selfUuid)) return false;

        boolean added = getPeerDirectory().runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
            boolean addedInner = d.getPeers().add(persistentPeerInfo.getUuid());
            if (addedInner) {
                jObjectManager.put(persistentPeerInfo, Optional.of(m.getName()));
                b.apply();
            }
            return addedInner;
        });
        return added;
    }

    public boolean removeHost(UUID host) {
        boolean removed = getPeerDirectory().runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d, b, v) -> {
            boolean removedInner = d.getPeers().remove(host);
            if (removedInner) {
                _persistentData.runWriteLocked(pd -> pd.getInitialSyncDone().remove(host));
                getPeer(host).runWriteLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (mp, dp, bp, vp) -> {
                    mp.removeRef(m.getName());
                    return null;
                });
                b.apply();
            }
            return removedInner;
        });
        return removed;
    }

    private void updateCerts() {
        try {
            getPeerDirectory().runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                peerTrustManager.reloadTrustManagerHosts(getHostsNoNulls());
                // Fixme:? I don't think it should be needed with custom trust store
                // but it doesn't work?
                rpcClientFactory.dropCache();
                return null;
            });
        } catch (Exception ex) {
            Log.error("Error when refreshing certificates, will retry", ex);
            pushPeerUpdates();
        }
    }

    public boolean existsHost(UUID uuid) {
        return getPeerDirectory().runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.getPeers().contains(uuid));
    }

    public PersistentPeerInfo getHost(UUID uuid) {
        return getPeer(uuid).runReadLocked(JObject.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d);
    }

    public KeyPair getSelfKeypair() {
        return _persistentData.runReadLocked(PersistentRemoteHostsData::getSelfKeyPair);
    }

    public X509Certificate getSelfCertificate() {
        return _persistentData.runReadLocked(PersistentRemoteHostsData::getSelfCertificate);
    }

    // Returns true if host's initial sync wasn't done before, and marks it as done
    public boolean markInitialSyncDone(UUID connectedHost) {
        return _persistentData.runWriteLocked(d -> d.getInitialSyncDone().add(connectedHost));
    }

}
