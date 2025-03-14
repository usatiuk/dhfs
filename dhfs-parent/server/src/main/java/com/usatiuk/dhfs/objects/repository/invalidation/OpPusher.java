package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.RemoteObjectMeta;
import com.usatiuk.dhfs.objects.RemoteTransaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class OpPusher {
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager txm;
    @Inject
    RemoteTransaction remoteTransaction;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;

    public void doPush(InvalidationQueueEntry entry) {
        List<Op> info = txm.run(() -> {
            var obj = curTx.get(JData.class, entry.key()).orElse(null);
            switch (obj) {
                case RemoteObjectMeta remote -> {
                    return List.of(new IndexUpdateOp(entry.key(), remote.changelog()));
                }
                case JKleppmannTreePersistentData pd -> {
                    var tree = jKleppmannTreeManager.getTree(pd.key());
                    if (entry.forced())
                        tree.recordBootstrap(entry.peer());

                    if (!tree.hasPendingOpsForHost(entry.peer()))
                        return null;

                    var ops = tree.getPendingOpsForHost(entry.peer(), 1);

                    if (tree.hasPendingOpsForHost(entry.peer()))
                        invalidationQueueService.pushInvalidationToOne(entry.peer(), pd.key());

                    return ops;
                }
                case null,
                     default -> {
                    return null;
                }
            }
        });
        if (info == null) {
            return;
        }
        remoteObjectServiceClient.pushOps(entry.peer(), info);
        txm.run(() -> {
            var obj = curTx.get(JData.class, entry.key()).orElse(null);
            switch (obj) {
                case JKleppmannTreePersistentData pd: {
                    var tree = jKleppmannTreeManager.getTree(pd.key());
                    for (var op : info) {
                        tree.commitOpForHost(entry.peer(), op);
                    }
                    break;
                }
                case null:
                default:
            }
        });

    }
}
