package com.usatiuk.dhfs.repository.peersync;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.RemoteTransaction;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.repository.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.objects.transaction.LockingStrategy;
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

    private JKleppmannTreeManager.JKleppmannTree getTreeW() {
        return jKleppmannTreeManager.getTree(TREE_KEY);
    }

    private JKleppmannTreeManager.JKleppmannTree getTreeR() {
        return jKleppmannTreeManager.getTree(TREE_KEY, LockingStrategy.OPTIMISTIC);
    }

    public Optional<PeerInfo> getPeerInfoImpl(JObjectKey key) {
        return jObjectTxManager.run(() -> {
            return curTx.get(JKleppmannTreeNode.class, key).flatMap(node -> {
                var meta = (JKleppmannTreeNodeMetaPeer) node.meta();
                return remoteTx.getData(PeerInfo.class, meta.getPeerId());
            });
        });

    }

    public boolean existsPeer(PeerId peer) {
        return jObjectTxManager.run(() -> {
            var gotKey = getTreeR().traverse(List.of(JKleppmannTreeNodeMetaPeer.peerIdToNodeId(peer).name()));
            if (gotKey == null) {
                return false;
            }
            return true;
        });
    }

    public Optional<PeerInfo> getPeerInfo(PeerId peer) {
        return jObjectTxManager.run(() -> {
            var gotKey = getTreeR().traverse(List.of(JKleppmannTreeNodeMetaPeer.peerIdToNodeId(peer).name()));
            if (gotKey == null) {
                return Optional.empty();
            }
            return curTx.get(JKleppmannTreeNode.class, gotKey).flatMap(node -> {
                var meta = (JKleppmannTreeNodeMetaPeer) node.meta();
                return remoteTx.getData(PeerInfo.class, meta.getPeerId());
            });
        });
    }

    public List<PeerInfo> getPeers() {
        return jObjectTxManager.run(() -> {
            var gotKey = getTreeR().traverse(List.of());
            return curTx.get(JKleppmannTreeNode.class, gotKey).map(
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
            var gotKey = getTreeR().traverse(List.of());
            return curTx.get(JKleppmannTreeNode.class, gotKey).map(
                            node -> node.children().keySet().stream()
                                    .map(JObjectKey::of).map(this::getPeerInfoImpl)
                                    .filter(o -> {
                                        if (o.isEmpty())
                                            Log.warnv("Could not get peer info for peer {0}", o);
                                        return o.isPresent();
                                    }).map(Optional::get).filter(
                                            peerInfo -> !peerInfo.id().equals(persistentPeerDataService.getSelfUuid())).toList())
                    .orElseThrow();
        });
    }

    public void putPeer(PeerId id, byte[] cert) {
        jObjectTxManager.run(() -> {
            var parent = getTreeW().traverse(List.of());
            var newPeerInfo = new PeerInfo(id, cert);
            remoteTx.putData(newPeerInfo);
            getTreeW().move(parent, new JKleppmannTreeNodeMetaPeer(newPeerInfo.id()), JKleppmannTreeNodeMetaPeer.peerIdToNodeId(newPeerInfo.id()));
        });
    }

    public void removePeer(PeerId id) {
        jObjectTxManager.run(() -> {
            var gotKey = getTreeR().traverse(List.of(JKleppmannTreeNodeMetaPeer.peerIdToNodeId(id).name()));
            if (gotKey == null) {
                return;
            }
            var node = curTx.get(JKleppmannTreeNode.class, gotKey).orElse(null);
            if (node == null) {
                Log.warn("Peer " + id + " not found in the tree");
                return;
            }
            getTreeW().trash(node.meta(), node.key());
            curTx.onCommit(persistentPeerDataService::updateCerts);
        });
    }
}
