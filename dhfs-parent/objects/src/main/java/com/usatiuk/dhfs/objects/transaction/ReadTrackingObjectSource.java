package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReadTrackingObjectSource implements TransactionObjectSource {
    private final TransactionObjectSource _delegate;

    private final Map<JObjectKey, TransactionObject<?>> _readSet = new HashMap<>();

    public ReadTrackingObjectSource(TransactionObjectSource delegate) {
        _delegate = delegate;
    }

    public Map<JObjectKey, TransactionObject<?>> getRead() {
        return Collections.unmodifiableMap(_readSet);
    }

    @Override
    public <T extends JData> TransactionObject<T> get(Class<T> type, JObjectKey key) {
        var got = _readSet.get(key);

        if (got == null) {
            var read = _delegate.get(type, key);
            _readSet.put(key, read);
            return read;
        }

        got.data().ifPresent(data -> {
            if (!type.isInstance(data))
                throw new IllegalStateException("Type mismatch for " + got + ": expected " + type + ", got " + data.getClass());
        });

        return (TransactionObject<T>) got;
    }

    @Override
    public <T extends JData> TransactionObject<T> getWriteLocked(Class<T> type, JObjectKey key) {
        var got = _readSet.get(key);

        if (got == null) {
            var read = _delegate.getWriteLocked(type, key);
            _readSet.put(key, read);
            return read;
        }

        got.data().ifPresent(data -> {
            if (!type.isInstance(data))
                throw new IllegalStateException("Type mismatch for " + got + ": expected " + type + ", got " + data.getClass());
        });

        return (TransactionObject<T>) got;
    }
}
