package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DeleterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;

    @Inject
    ObjectAllocator alloc;

    private boolean canDelete(JDataRefcounted data) {
        return !data.getFrozen() && data.getRefsFrom().isEmpty();
    }

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        if (canDelete(refCur)) {
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
