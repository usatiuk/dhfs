package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Collection;
import java.util.Map;

// The transaction interface actually used by user code to retrieve objects
public interface TransactionPrivate extends Transaction {
    Collection<TxRecord.TxObjectRecord<?>> writes();

    Map<JObjectKey, ReadTrackingObjectSource.TxReadObject<?>> reads();
}
