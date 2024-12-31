package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public interface TransactionObjectSource {
    <T extends JData> TransactionObject<T> get(Class<T> type, JObjectKey key);

    <T extends JData> TransactionObject<T> getWriteLocked(Class<T> type, JObjectKey key);
}
