package com.usatiuk.dhfs.peersync;

import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeHolder;
import com.usatiuk.dhfs.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class PeerInfoService {
    public static final JObjectKey TREE_KEY = JObjectKey.of("peers");
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager jObjectTxManager;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    RemoteTransaction remoteTx;

    private JKleppmannTreeManager.JKleppmannTree getTree() {
        return jKleppmannTreeManager.getTree(TREE_KEY, () -> null);
    }

    public Optional<PeerInfo> getPeerInfoImpl(JObjectKey key) {
        return jObjectTxManager.run(() -> {
            return curTx.get(JKleppmannTreeNodeHolder.class, key).map(JKleppmannTreeNodeHolder::node).flatMap(node -> {
                var meta = (JKleppmannTreeNodeMetaPeer) node.meta();
                return remoteTx.getData(PeerInfo.class, meta.peerId());
            });
        });

    }

    public boolean existsPeer(PeerId peer) {
        return jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of(JKleppmannTreeNodeMetaPeer.peerIdToNodeId(peer).value()));
            if (gotKey == null) {
                return false;
            }
            return true;
        });
    }

    public Optional<PeerInfo> getPeerInfo(PeerId peer) {
        return jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of(JKleppmannTreeNodeMetaPeer.peerIdToNodeId(peer).value()));
            if (gotKey == null) {
                return Optional.empty();
            }
            return curTx.get(JKleppmannTreeNodeHolder.class, gotKey).map(JKleppmannTreeNodeHolder::node).flatMap(node -> {
                var meta = (JKleppmannTreeNodeMetaPeer) node.meta();
                return remoteTx.getData(PeerInfo.class, meta.peerId());
            });
        });
    }

    public List<PeerInfo> getPeers() {
        return jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of());
            return curTx.get(JKleppmannTreeNodeHolder.class, gotKey).map(JKleppmannTreeNodeHolder::node).map(
                            node -> node.children().keySet().stream()
                                    .map(JObjectKey::of).map(this::getPeerInfoImpl)
                                    .filter(o -> {
                                        if (o.isEmpty())
                                            Log.warnv("Could not get peer info for peer {0}", o);
                                        return o.isPresent();
                                    }).map(Optional::get).toList())
                    .orElseThrow();
        });
    }

    public List<PeerInfo> getPeersNoSelf() {
        return jObjectTxManager.run(() -> {
            return getPeers().stream().filter(
                    peerInfo -> !peerInfo.id().equals(persistentPeerDataService.getSelfUuid())).toList();
        });
    }

    public List<PeerInfo> getSynchronizedPeers() {
        return jObjectTxManager.run(() -> {
            return getPeers().stream().filter(pi -> {
                if (pi.id().equals(persistentPeerDataService.getSelfUuid())) {
                    return true;
                }
                return persistentPeerDataService.isInitialSyncDone(pi.id());
            }).toList();
        });
    }

    public List<PeerInfo> getSynchronizedPeersNoSelf() {
        return jObjectTxManager.run(() -> {
            return getPeersNoSelf().stream().filter(pi -> {
                return persistentPeerDataService.isInitialSyncDone(pi.id());
            }).toList();
        });
    }

    public void putPeer(PeerId id, byte[] cert) {
        jObjectTxManager.run(() -> {
            var parent = getTree().traverse(List.of());
            var newPeerInfo = new PeerInfo(id, cert);
            remoteTx.putData(newPeerInfo);
            getTree().move(parent, new JKleppmannTreeNodeMetaPeer(newPeerInfo.id()), JKleppmannTreeNodeMetaPeer.peerIdToNodeId(newPeerInfo.id()));
        });
    }

    public void removePeer(PeerId id) {
        jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of(JKleppmannTreeNodeMetaPeer.peerIdToNodeId(id).value()));
            if (gotKey == null) {
                return;
            }
            var node = curTx.get(JKleppmannTreeNodeHolder.class, gotKey).map(JKleppmannTreeNodeHolder::node).orElse(null);
            if (node == null) {
                Log.warn("Peer " + id + " not found in the tree");
                return;
            }
            getTree().trash(node.meta(), node.key());
            curTx.onCommit(persistentPeerDataService::updateCerts);
        });
    }
}
