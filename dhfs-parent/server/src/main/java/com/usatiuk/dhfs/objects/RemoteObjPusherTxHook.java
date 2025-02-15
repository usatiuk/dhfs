package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RemoteObjPusherTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    InvalidationQueueService invalidationQueueService;

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        boolean invalidate = switch (cur) {
            case RemoteObjectMeta remote -> !remote.changelog().equals(((RemoteObjectMeta) old).changelog());
            case JKleppmannTreePersistentData pd -> !pd.queues().equals(((JKleppmannTreePersistentData) old).queues());
            default -> false;
        };

        if (invalidate) {
            invalidationQueueService.pushInvalidationToAll(cur.key());
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectMeta remote)) {
            return;
        }

        invalidationQueueService.pushInvalidationToAll(remote.key());
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectMeta remote)) {
            return;
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
