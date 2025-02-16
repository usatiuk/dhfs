package com.usatiuk.dhfs.objects.transaction;

public interface TransactionHandle {
    long getId();

    void onFlush(Runnable runnable);
}
