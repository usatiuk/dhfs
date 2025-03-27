package com.usatiuk.dhfs.jmap;

import com.usatiuk.dhfs.JDataRefcounted;
import com.usatiuk.dhfs.RemoteObjectDataWrapper;
import com.usatiuk.dhfs.RemoteObjectMeta;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (cur instanceof RemoteObjectDataWrapper dw) {
            if (dw.data() instanceof JMapHolder he) {
                helper.deleteAll(he);
                return;
            }
        }

        if (cur instanceof JMapHolder he) {
            helper.deleteAll(he);
            return;
        }
    }

}
