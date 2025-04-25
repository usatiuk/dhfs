package com.usatiuk.dhfs.refcount;

import com.usatiuk.dhfs.remoteobj.RemoteObjectDeleter;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class DeleterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    RemoteObjectDeleter remoteObjectDeleter;

    private boolean canDelete(JDataRefcounted data) {
        return !data.frozen() && data.refsFrom().isEmpty();
    }

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }
        if (canDelete(refCur)) {
            if (refCur instanceof RemoteObjectMeta ro) {
                curTx.onCommit(() -> remoteObjectDeleter.putDeletionCandidate(ro));
                return;
            }
            Log.trace("Deleting object on change: " + key);
            curTx.delete(key);
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        if (canDelete(refCur)) {
            if (refCur instanceof RemoteObjectMeta ro) {
                curTx.onCommit(() -> remoteObjectDeleter.putDeletionCandidate(ro));
                return;
            }
            Log.warn("Deleting object on creation: " + key);
            curTx.delete(key);
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        if (!canDelete(refCur)) {
            throw new IllegalStateException("Deleting object with refs: " + key);
        }
    }

    @Override
    public int getPriority() {
        return 200;
    }
}
