package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.snapshot.SnapshotManager;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

import java.util.Collection;
import java.util.Map;

// The transaction interface actually used by user code to retrieve objects
public interface TransactionPrivate extends Transaction, TransactionHandlePrivate, AutoCloseableNoThrow {
    Collection<TxRecord.TxObjectRecord<?>> drainNewWrites();

    Map<JObjectKey, TransactionObject<?>> reads();

    ReadTrackingTransactionObjectSource readSource();

    Collection<Runnable> getOnCommit();

    SnapshotManager.Snapshot snapshot();
}
