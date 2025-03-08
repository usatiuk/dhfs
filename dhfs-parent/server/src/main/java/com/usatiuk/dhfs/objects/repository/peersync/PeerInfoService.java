package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.RemoteTransaction;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class PeerInfoService {
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
        return jKleppmannTreeManager.getTree(JObjectKey.of("peers"));
    }

    public Optional<PeerInfo> getPeerInfo(PeerId peer) {
        return jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of(peer.toString()));
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
            var gotKey = getTree().traverse(List.of());
            return curTx.get(JKleppmannTreeNode.class, gotKey).map(
                            node -> node.children().keySet().stream()
                                    .map(PeerId::of).map(this::getPeerInfo)
                                    .map(Optional::get).toList())
                    .orElseThrow();
        });
    }

    public List<PeerInfo> getPeersNoSelf() {
        return jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of());
            return curTx.get(JKleppmannTreeNode.class, gotKey).map(
                            node -> node.children().keySet().stream()
                                    .map(PeerId::of).map(this::getPeerInfo)
                                    .map(Optional::get).filter(
                                            peerInfo -> !peerInfo.id().equals(persistentPeerDataService.getSelfUuid())).toList())
                    .orElseThrow();
        });
    }

    public void putPeer(PeerId id, byte[] cert) {
        jObjectTxManager.run(() -> {
            var parent = getTree().traverse(List.of());
            var newPeerInfo = new PeerInfo(id, cert);
            remoteTx.putData(newPeerInfo);
            getTree().move(parent, new JKleppmannTreeNodeMetaPeer(newPeerInfo.id()), getTree().getNewNodeId());
        });
    }

    public void removePeer(PeerId id) {
        jObjectTxManager.run(() -> {
            var gotKey = getTree().traverse(List.of(id.toString()));
            if (gotKey == null) {
                return;
            }
            var meta = curTx.get(JKleppmannTreeNode.class, gotKey).map(node -> (JKleppmannTreeNodeMetaPeer) node.meta()).orElse(null);
            if (meta == null) {
                Log.warn("Peer " + id + " not found in the tree");
                return;
            }
            getTree().trash(meta, id.toJObjectKey());
        });
    }
}
