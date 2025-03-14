package com.usatiuk.dhfs.objects.jmap;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;

@ApplicationScoped
public class JMapHolderRefcounterTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    JMapHelper helper;

    private JDataRefcounted getRef(JObjectKey key) {
        var found = curTx.get(JDataRefcounted.class, key).orElse(null);

        if (found != null) {
            return found;
        }

        return new RemoteObjectMeta(key);
    }

    private <K extends JMapKey & Comparable<K>> void onDeleteImpl(JMapHolder<K> he) {
        ArrayList<K> collectedKeys = new ArrayList<>();
        try (var it = helper.getIterator(he, IteratorStart.GE)) {
            while (it.hasNext()) {
                var curKey = it.peekNextKey();
                collectedKeys.add(curKey);
                it.skip();
            }
        }

        for (var curKey : collectedKeys) {
            helper.delete(he, curKey);
            Log.tracev("Removed map entry {0} to {1}", he.key(), curKey);
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (cur instanceof RemoteObjectDataWrapper dw) {
            if (dw.data() instanceof JMapHolder he) {
                onDeleteImpl(he);
                return;
            }
        }

        if (cur instanceof JMapHolder he) {
            onDeleteImpl(he);
            return;
        }
    }

}
