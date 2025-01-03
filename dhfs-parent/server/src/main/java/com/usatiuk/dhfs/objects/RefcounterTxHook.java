package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RefcounterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;

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
                var referenced = curTx.get(JDataRefcounted.class, curRef).orElse(null);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(key)));
            }
        }

        for (var oldRef : oldRefs) {
            if (!curRefs.contains(oldRef)) {
                var referenced = curTx.get(JDataRefcounted.class, oldRef).orElse(null);
                curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(key)));
            }
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var newRef : refCur.collectRefsTo()) {
            var referenced = curTx.get(JDataRefcounted.class, newRef).orElse(null);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().plus(key)));
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var removedRef : refCur.collectRefsTo()) {
            var referenced = curTx.get(JDataRefcounted.class, removedRef).orElse(null);
            curTx.put(referenced.withRefsFrom(referenced.refsFrom().minus(key)));
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
