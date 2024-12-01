package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;

public interface TransactionManager {
    void begin();

    void commit();

    void rollback();

    Transaction current();
}
