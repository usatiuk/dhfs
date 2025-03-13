package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.snapshot.Snapshot;
import com.usatiuk.dhfs.objects.snapshot.SnapshotManager;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

// The transaction interface actually used by user code to retrieve objects
public interface TransactionPrivate extends Transaction, TransactionHandlePrivate, AutoCloseableNoThrow {
    Collection<TxRecord.TxObjectRecord<?>> drainNewWrites();

    Map<JObjectKey, TransactionObject<?>> reads();

    <T extends JData> Optional<T> getFromSource(Class<T> type, JObjectKey key);

    Collection<Runnable> getOnCommit();

    Snapshot<JObjectKey, JDataVersionedWrapper> snapshot();
}
