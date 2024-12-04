package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JDataAllocVersionProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class TransactionObjectAllocVersionProvider implements JDataAllocVersionProvider {
    @Inject
    Transaction transaction;

    public long getVersion() {
        return transaction.getId();
    }

}
