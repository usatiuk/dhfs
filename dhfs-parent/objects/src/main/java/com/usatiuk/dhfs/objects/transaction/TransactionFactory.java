package com.usatiuk.dhfs.objects.transaction;

public interface TransactionFactory {
    TransactionPrivate createTransaction(long id, TransactionObjectSource source);
}
