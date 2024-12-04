package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Optional;

public interface TransactionObjectSource {
    <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key);

    <T extends JData> Optional<TransactionObject<T>> getWriteLocked(Class<T> type, JObjectKey key);
}
