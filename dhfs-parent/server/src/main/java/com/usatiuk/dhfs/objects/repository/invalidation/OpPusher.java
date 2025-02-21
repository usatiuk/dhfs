package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.RemoteObjectMeta;
import com.usatiuk.dhfs.objects.RemoteTransaction;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
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

    public void doPush(InvalidationQueueEntry entry) {
        Op info = txm.run(() -> {
            var obj = curTx.get(JData.class, entry.key()).orElse(null);
            switch (obj) {
                case RemoteObjectMeta remote -> {
                    return new IndexUpdateOp(entry.key(), remote.changelog());
                }
                case JKleppmannTreePersistentData pd -> {
                    var maybeQueue = pd.queues().get(entry.peer());
                    if (maybeQueue == null || maybeQueue.isEmpty()) {
                        return null;
                    }
                    var ret = new JKleppmannTreeOpWrapper(entry.key(), pd.queues().get(entry.peer()).firstEntry().getValue());
                    var newPd = pd.withQueues(pd.queues().plus(entry.peer(), pd.queues().get(entry.peer()).minus(ret.op().timestamp())));
                    curTx.put(newPd);
                    if (!newPd.queues().get(entry.peer()).isEmpty())
                        invalidationQueueService.pushInvalidationToOne(entry.peer(), pd.key());
                    return ret;
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
        remoteObjectServiceClient.pushOps(entry.peer(), List.of(info));
    }
}
