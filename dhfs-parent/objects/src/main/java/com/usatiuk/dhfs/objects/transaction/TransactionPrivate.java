package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JObjectKey;

import java.util.Collection;
import java.util.Map;

// The transaction interface actually used by user code to retrieve objects
public interface TransactionPrivate extends Transaction {
    Collection<TxRecord.TxObjectRecord<?>> drainNewWrites();

    Map<JObjectKey, TransactionObject<?>> reads();
}
