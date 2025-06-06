package com.usatiuk.dhfs.jmap;

import com.usatiuk.dhfs.refcount.JDataRefcounted;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class JMapRefcounterTxHook implements PreCommitTxHook {
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
        if (!(cur instanceof JMapEntry<?> me)) {
            return;
        }

        var oldMe = (JMapEntry<?>) old;
        var oldRef = oldMe.ref();
        var curRef = me.ref();
        var referencedOld = getRef(oldRef);
        curTx.put(referencedOld.withRefsFrom(referencedOld.refsFrom().minus(new JMapRef(me.holder(), me.selfKey()))));
        var referencedCur = getRef(curRef);
        curTx.put(referencedCur.withRefsFrom(referencedCur.refsFrom().plus(new JMapRef(me.holder(), me.selfKey()))));
        Log.tracev("Removed ref from {0} to {1}, added ref to {2}", key, oldRef, curRef);
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof JMapEntry<?> me)) {
            return;
        }

        var curRef = me.ref();
        var referencedCur = getRef(curRef);
        curTx.put(referencedCur.withRefsFrom(referencedCur.refsFrom().plus(new JMapRef(me.holder(), me.selfKey()))));
        Log.tracev("Added ref from {0} to {1}", key, curRef);
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof JMapEntry<?> me)) {
            return;
        }

        var oldRef = me.ref();
        var referencedOld = getRef(oldRef);
        curTx.put(referencedOld.withRefsFrom(referencedOld.refsFrom().minus(new JMapRef(me.holder(), me.selfKey()))));
        Log.tracev("Removed ref from {0} to {1}", key, oldRef);
    }

}
