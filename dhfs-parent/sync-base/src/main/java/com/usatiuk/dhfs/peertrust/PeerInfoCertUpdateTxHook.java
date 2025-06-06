package com.usatiuk.dhfs.peertrust;

import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeHolder;
import com.usatiuk.dhfs.peersync.PeerInfo;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.dhfs.remoteobj.RemoteObjectDataWrapper;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Automatically refreshes certificates in the trust manager for peers when their info is updated.
 */
@Singleton
public class PeerInfoCertUpdateTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    InvalidationQueueService invalidationQueueService;

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        // We also need to force pushing invalidation to all, in case our node acts as a "middleman"
        // connecting two other nodes
        // TODO: Can there be a prettier way to do this? (e.g. more general proxying of ops?)
        if (cur instanceof JKleppmannTreeNodeHolder n) {
            if (n.key().value().equals("peers_jt_root")) {
                // TODO: This is kinda sucky
                Log.infov("Changed peer tree root: {0} to {1}", key, cur);

                curTx.onCommit(() -> persistentPeerDataService.updateCerts());
                curTx.onCommit(() -> invalidationQueueService.pushInvalidationToAll(PeerInfoService.TREE_KEY));
                if (!(old instanceof JKleppmannTreeNodeHolder oldNode))
                    throw new IllegalStateException("Old node is not a tree node");

                for (var curRef : oldNode.node().children().entrySet()) {
                    if (!n.node().children().containsKey(curRef.getKey())) {
                        Log.infov("Will reset sync state for {0}", curRef.getValue());
                        persistentPeerDataService.resetInitialSyncDone(JKleppmannTreeNodeMetaPeer.nodeIdToPeerId(curRef.getValue()));
                    }
                }
                return;
            }
        }

        if (!(cur instanceof RemoteObjectDataWrapper remote)) {
            return;
        }

        if (!(remote.data() instanceof PeerInfo curPi))
            return;

        var oldPi = (PeerInfo) ((RemoteObjectDataWrapper) old).data();

        if (oldPi.kickCounterSum() != curPi.kickCounterSum()) {
            Log.warnv("Peer kicked out: {0} to {1}", key, cur);
            persistentPeerDataService.resetInitialSyncDone(curPi.id());
        }

        Log.infov("Changed peer info: {0} to {1}", key, cur);

        curTx.onCommit(() -> invalidationQueueService.pushInvalidationToAll(key));
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
