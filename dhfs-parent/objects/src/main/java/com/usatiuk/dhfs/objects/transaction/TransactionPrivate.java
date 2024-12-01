package com.usatiuk.dhfs.objects.transaction;

import java.util.Collection;

// The transaction interface actually used by user code to retrieve objects
public interface TransactionPrivate extends Transaction{
    long getId();
    Collection<TxRecord.TxObjectRecord<?>> drain();
}
