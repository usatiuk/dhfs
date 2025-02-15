package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
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
                remoteObjectDeleter.putDeletionCandidate(ro);
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
                remoteObjectDeleter.putDeletionCandidate(ro);
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
