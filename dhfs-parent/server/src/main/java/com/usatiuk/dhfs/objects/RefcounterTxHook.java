package com.usatiuk.dhfs.objects;

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
                var referenced = getRef(curRef.obj());
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(new JDataNormalRef(curRef.obj()))));
                Log.tracev("Added ref from {0} to {1}", key, curRef);
            }
        }

        for (var oldRef : oldRefs) {
            if (!curRefs.contains(oldRef)) {
                var referenced = getRef(oldRef.obj());
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(new JDataNormalRef(oldRef.obj()))));
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
            var referenced = getRef(newRef.obj());
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(new JDataNormalRef(newRef.obj()))));
            Log.tracev("Added ref from {0} to {1}", key, newRef);
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var removedRef : refCur.collectRefsTo()) {
            var referenced = getRef(removedRef.obj());
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(new JDataNormalRef(removedRef.obj()))));
            Log.tracev("Removed ref from {0} to {1}", key, removedRef);
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
