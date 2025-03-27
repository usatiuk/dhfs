package com.usatiuk.dhfs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.transaction.PreCommitTxHook;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RefcounterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;

    private JDataRefcounted getRef(JObjectKey key) {
        var found = curTx.get(JDataRefcounted.class, key).orElse(null);

        if (found != null) {
            return found;
        }

        return new RemoteObjectMeta(key);
    }

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }
        var refOld = (JDataRefcounted) old;

        var curRefs = refCur.collectRefsTo();
        var oldRefs = refOld.collectRefsTo();

        for (var curRef : curRefs) {
            if (!oldRefs.contains(curRef)) {
                var referenced = getRef(curRef);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(new JDataNormalRef(key))));
                Log.tracev("Added ref from {0} to {1}", key, curRef);
            }
        }

        for (var oldRef : oldRefs) {
            if (!curRefs.contains(oldRef)) {
                var referenced = getRef(oldRef);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(new JDataNormalRef(key))));
                Log.tracev("Removed ref from {0} to {1}", key, oldRef);
            }
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var newRef : refCur.collectRefsTo()) {
            var referenced = getRef(newRef);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(new JDataNormalRef(key))));
            Log.tracev("Added ref from {0} to {1}", key, newRef);
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var removedRef : refCur.collectRefsTo()) {
            var referenced = getRef(removedRef);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(new JDataNormalRef(key))));
            Log.tracev("Removed ref from {0} to {1}", key, removedRef);
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
