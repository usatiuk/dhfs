package com.usatiuk.dhfs.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.ShutdownChecker;
import com.usatiuk.dhfs.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.peerdiscovery.PeerAddressType;
import com.usatiuk.dhfs.peertrust.CertificateTools;
import com.usatiuk.dhfs.peertrust.PeerTrustManager;
import com.usatiuk.dhfs.rpc.RpcClientFactory;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import com.usatiuk.utils.SerializationHelper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.List;
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
    @Inject
    PeerManager peerManager;

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
                _selfCertificate = CertificateTools.certFromBytes(selfData.selfCertificate().toByteArray());
                _selfKeyPair = SerializationHelper.deserialize(selfData.selfKeyPair().toByteArray());
                return;
            } else {
                try {
                    _selfUuid = presetUuid.map(PeerId::of).orElseGet(() -> PeerId.of(UUID.randomUUID().toString()));
                    Log.info("Generating a key pair, please wait");
                    _selfKeyPair = CertificateTools.generateKeyPair();
                    _selfCertificate = CertificateTools.generateCertificate(_selfKeyPair, _selfUuid.toString());

                    curTx.put(new PersistentRemoteHostsData(_selfUuid, ByteString.copyFrom(_selfCertificate.getEncoded()), SerializationHelper.serialize(_selfKeyPair), HashTreePSet.empty(), HashTreePMap.empty()));
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
            curTx.onCommit(() -> peerManager.handleConnectionError(peerId));
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

    public List<IpPeerAddress> getPersistentPeerAddresses() {
        return txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            return data.persistentPeerAddress().values().stream().toList();
        });
    }

    public void addPersistentPeerAddress(PeerId peerId, IpPeerAddress address) {
        txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            var newData = data.persistentPeerAddress().plus(peerId, address.withType(PeerAddressType.WAN)); //TODO:
            curTx.put(data.withPersistentPeerAddress(newData));
        });
    }

    public void removePersistentPeerAddress(PeerId peerId) {
        txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            var newData = data.persistentPeerAddress().minus(peerId);
            curTx.put(data.withPersistentPeerAddress(newData));
        });
    }

    public IpPeerAddress getPersistentPeerAddress(PeerId peerId) {
        return txm.run(() -> {
            var data = curTx.get(PersistentRemoteHostsData.class, PersistentRemoteHostsData.KEY).orElse(null);
            if (data == null) throw new IllegalStateException("Self data not found");
            return data.persistentPeerAddress().get(peerId);
        });
    }
}
