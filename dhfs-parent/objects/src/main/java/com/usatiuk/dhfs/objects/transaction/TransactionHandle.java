package com.usatiuk.dhfs.objects.transaction;

public interface TransactionHandle {
    void onFlush(Runnable runnable);
}
