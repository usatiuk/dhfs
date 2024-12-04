package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;

import java.util.concurrent.locks.ReadWriteLock;

public interface TransactionObject<T extends JData> {
    T data();

    ReadWriteLock lock();
}
