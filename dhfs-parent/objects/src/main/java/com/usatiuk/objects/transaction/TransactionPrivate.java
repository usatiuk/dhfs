package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.utils.AutoCloseableNoThrow;

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
