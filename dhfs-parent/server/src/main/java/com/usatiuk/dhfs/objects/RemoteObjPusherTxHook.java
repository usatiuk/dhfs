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
            case RemoteObject<?> remote -> !remote.meta().changelog().equals(((RemoteObject) old).meta().changelog());
            case JKleppmannTreePersistentData pd -> !pd.queues().equals(((JKleppmannTreePersistentData) old).queues());
            default -> false;
        };

        if (invalidate) {
            invalidationQueueService.pushInvalidationToAll(cur.key());
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObject remote)) {
            return;
        }

        invalidationQueueService.pushInvalidationToAll(remote.key());
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObject remote)) {
            return;
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
