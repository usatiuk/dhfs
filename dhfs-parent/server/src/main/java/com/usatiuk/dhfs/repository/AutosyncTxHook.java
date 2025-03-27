package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.RemoteObjectMeta;
import com.usatiuk.dhfs.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.transaction.PreCommitTxHook;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
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
