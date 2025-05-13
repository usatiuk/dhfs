package com.usatiuk.dhfs.autosync;

import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Pre-commit hook to automatically download remote objects, if the option to download all objects is enabled.
 */
@Singleton
public class AutosyncTxHook implements PreCommitTxHook {
    @Inject
    Transaction curTx;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    AutosyncProcessor autosyncProcessor;

    @ConfigProperty(name = "dhfs.objects.autosync.download-all")
    boolean downloadAll;

    @Override
    public void onChange(JObjectKey key, JData old, JData cur) {
        if (!(cur instanceof RemoteObjectMeta meta))
            return;

        if (!meta.hasLocalData() && downloadAll) {
            curTx.onCommit(() -> autosyncProcessor.add(meta.key()));
        }
    }

    @Override
    public void onCreate(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectMeta meta))
            return;

        if (!meta.hasLocalData() && downloadAll) {
            curTx.onCommit(() -> autosyncProcessor.add(meta.key()));
        }
    }

    @Override
    public void onDelete(JObjectKey key, JData cur) {
        if (!(cur instanceof RemoteObjectMeta remote)) {
            return;
        }
    }

    @Override
    public int getPriority() {
        return 100;
    }
}
