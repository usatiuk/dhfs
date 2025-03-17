package com.usatiuk.dhfs.objects.repository.peertrust;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.RemoteObjectDataWrapper;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.dhfs.objects.transaction.PreCommitTxHook;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class PeerInfoCertUpdateTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        if (cur instanceof JKleppmannTreeNode n) {
            if (n.key().name().equals("peers_jt_root")) {
                // TODO: This is kinda sucky
                Log.infov("Changed peer tree root: {0} to {1}", key, cur);

                curTx.onCommit(() -> persistentPeerDataService.updateCerts());
                if (!(old instanceof JKleppmannTreeNode oldNode))
                    throw new IllegalStateException("Old node is not a tree node");

                for (var curRef : oldNode.children().entrySet()) {
                    if (!n.children().containsKey(curRef.getKey())) {
                        Log.infov("Will reset sync state for {0}", curRef.getValue());
                        curTx.onCommit(() -> persistentPeerDataService.resetInitialSyncDone(JKleppmannTreeNodeMetaPeer.nodeIdToPeerId(curRef.getValue())));
                    }
                }
                return;
            }
        }

        if (!(cur instanceof RemoteObjectDataWrapper remote)) {
            return;
        }

        if (!(remote.data() instanceof PeerInfo))
            return;

        Log.infov("Changed peer info: {0} to {1}", key, cur);

        curTx.onCommit(() -> persistentPeerDataService.updateCerts());
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectDataWrapper remote)) {
            return;
        }

        if (!(remote.data() instanceof PeerInfo))
            return;

        Log.infov("Created peer info: {0}, {1}", key, cur);

        curTx.onCommit(() -> persistentPeerDataService.updateCerts());
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectDataWrapper remote)) {
            return;
        }

        if (!(remote.data() instanceof PeerInfo))
            return;

        Log.infov("Deleted peer info: {0}, {1}", key, cur);

        curTx.onCommit(() -> persistentPeerDataService.updateCerts());
    }

}
