package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RefcounterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;

    private JDataRefcounted getRef(JDataRefcounted cur, JObjectKey key) {
        var found = curTx.get(JDataRefcounted.class, key).orElse(null);

        if (found != null) {
            return found;
        }

        if (cur instanceof RemoteObjectDataWrapper<?> || cur instanceof JKleppmannTreeNode) {
            // FIXME:
            return new RemoteObjectMeta(key);
        } else {
            return found;
        }

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
                var referenced = getRef(refCur, curRef);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(key)));
                Log.tracev("Added ref from {0} to {1}", key, curRef);
            }
        }

        for (var oldRef : oldRefs) {
            if (!curRefs.contains(oldRef)) {
                var referenced = getRef(refCur, oldRef);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(key)));
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
            var referenced = getRef(refCur, newRef);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(key)));
            Log.tracev("Added ref from {0} to {1}", key, newRef);
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var removedRef : refCur.collectRefsTo()) {
            var referenced = getRef(refCur, removedRef);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(key)));
            Log.tracev("Removed ref from {0} to {1}", key, removedRef);
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
