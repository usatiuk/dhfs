package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Set;

@ApplicationScoped
public class RefcounterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;

    @Inject
    ObjectAllocator alloc;

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }
        var refOld = (JDataRefcounted) old;

        for (var newRef : CollectionUtils.subtract(refCur.collectRefsTo(), refOld.collectRefsTo())) {
            var referenced = curTx.get(JDataRefcounted.class, newRef).orElse(null);
            referenced.setRefsFrom(CollectionUtils.union(referenced.getRefsFrom(), Set.of(key)));
        }

        for (var removedRef : CollectionUtils.subtract(refOld.collectRefsTo(), refCur.collectRefsTo())) {
            var referenced = curTx.get(JDataRefcounted.class, removedRef).orElse(null);
            referenced.setRefsFrom(CollectionUtils.subtract(referenced.getRefsFrom(), Set.of(key)));
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }

        for (var newRef : refCur.collectRefsTo()) {
            var referenced = curTx.get(JDataRefcounted.class, newRef).orElse(null);
            referenced.setRefsFrom(CollectionUtils.union(referenced.getRefsFrom(), Set.of(key)));
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JDataRefcounted refCur)) {
            return;
        }


        for (var removedRef : refCur.collectRefsTo()) {
            var referenced = curTx.get(JDataRefcounted.class, removedRef).orElse(null);
            referenced.setRefsFrom(CollectionUtils.subtract(referenced.getRefsFrom(), Set.of(key)));
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
