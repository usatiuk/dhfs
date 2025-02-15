package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.*;
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

    public void doPush(PeerId op, JObjectKey key) {
        Op info = txm.run(() -> {
            var obj = curTx.get(JData.class, key).orElse(null);
            switch (obj) {
                case RemoteObjectMeta remote -> {
                    return new IndexUpdateOp(key, remote.changelog());
                }
                case JKleppmannTreePersistentData pd -> {
                    var maybeQueue = pd.queues().get(op);
                    if(maybeQueue == null || maybeQueue.isEmpty()) {
                        return null;
                    }
                    var ret = new JKleppmannTreeOpWrapper(key, pd.queues().get(op).firstEntry().getValue());
                    var newPd = pd.withQueues(pd.queues().plus(op, pd.queues().get(op).minus(ret.op().timestamp())));
                    curTx.put(newPd);
                    if (!newPd.queues().get(op).isEmpty())
                        invalidationQueueService.pushInvalidationToOne(op, pd.key());
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
        remoteObjectServiceClient.pushOps(op, List.of(info));
    }
}
