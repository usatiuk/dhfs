package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Transaction hook to automatically notify {@link InvalidationQueueService} about changed objects.
 */
@Singleton
public class RemoteObjPusherTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    InvalidationQueueService invalidationQueueService;

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        boolean invalidate = switch (cur) {
            case RemoteObjectMeta remote -> remote.changelog() != ((RemoteObjectMeta) old).changelog();
            case JKleppmannTreePersistentData pd -> pd.queues() != ((JKleppmannTreePersistentData) old).queues();
            default -> false;
        };

        if (invalidate) {
            curTx.onCommit(() -> invalidationQueueService.pushInvalidationToAll(cur.key()));
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectMeta remote)) {
            return;
        }

        curTx.onCommit(() -> invalidationQueueService.pushInvalidationToAll(remote.key()));
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
