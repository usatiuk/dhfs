package com.usatiuk.objects.transaction;

public interface TransactionHandle {
    void onFlush(Runnable runnable);
}
