package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public interface TransactionObjectSource {
    <T extends JData> TransactionObject<T> get(Class<T> type, JObjectKey key);

    <T extends JData> TransactionObject<T> getWriteLocked(Class<T> type, JObjectKey key);
}
