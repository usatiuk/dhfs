package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.jmap.JMapEntry;
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
        if (cur instanceof JMapEntry<?> me) {
            var oldMe = (JMapEntry<?>) old;
            var oldRef = oldMe.ref();
            var curRef = me.ref();
            var referencedOld = getRef(oldRef);
            curTx.put(referencedOld.withRefsFrom(referencedOld.refsFrom().minus(key)));
            var referencedCur = getRef(curRef);
            curTx.put(referencedCur.withRefsFrom(referencedCur.refsFrom().plus(key)));
            Log.tracev("Removed ref from {0} to {1}, added ref to {2}", key, oldRef, curRef);
            return;
        }

        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }
        var refOld = (JDataRefcounted) old;

        var curRefs = refCur.collectRefsTo();
        var oldRefs = refOld.collectRefsTo();

        for (var curRef : curRefs) {
            if (!oldRefs.contains(curRef)) {
                var referenced = getRef(curRef);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(key)));
                Log.tracev("Added ref from {0} to {1}", key, curRef);
            }
        }

        for (var oldRef : oldRefs) {
            if (!curRefs.contains(oldRef)) {
                var referenced = getRef(oldRef);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(key)));
                Log.tracev("Removed ref from {0} to {1}", key, oldRef);
            }
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (cur instanceof JMapEntry<?> me) {
            var curRef = me.ref();
            var referencedCur = getRef(curRef);
            curTx.put(referencedCur.withRefsFrom(referencedCur.refsFrom().plus(key)));
            Log.tracev("Added ref from {0} to {1}", key, curRef);
            return;
        }

        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var newRef : refCur.collectRefsTo()) {
            var referenced = getRef(newRef);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(key)));
            Log.tracev("Added ref from {0} to {1}", key, newRef);
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (cur instanceof JMapEntry<?> me) {
            var oldRef = me.ref();
            var referencedOld = getRef(oldRef);
            curTx.put(referencedOld.withRefsFrom(referencedOld.refsFrom().minus(key)));
            Log.tracev("Removed ref from {0} to {1}", key, oldRef);
            return;
        }

        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var removedRef : refCur.collectRefsTo()) {
            var referenced = getRef(removedRef);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(key)));
            Log.tracev("Removed ref from {0} to {1}", key, removedRef);
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
