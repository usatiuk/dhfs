package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public interface ReadTrackingTransactionObjectSource extends AutoCloseableNoThrow {
    <T extends JData> Optional<T> get(Class<T> type, JObjectKey key);

    <T extends JData> Optional<T> getWriteLocked(Class<T> type, JObjectKey key);

    Iterator<Pair<JObjectKey, JData>> getIterator(IteratorStart start, JObjectKey key);

    default Iterator<Pair<JObjectKey, JData>> getIterator(JObjectKey key) {
        return getIterator(IteratorStart.GE, key);
    }

    Map<JObjectKey, TransactionObject<?>> getRead();
}
