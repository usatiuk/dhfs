package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ReadTrackingObjectSource implements TransactionObjectSource {
    private final TransactionObjectSource _delegate;

    public interface TxReadObject<T extends JData> {}

    public record TxReadObjectNone<T extends JData>() implements TxReadObject<T> {}

    public record TxReadObjectSome<T extends JData>(TransactionObject<T> obj) implements TxReadObject<T> {}

    private final Map<JObjectKey, TxReadObject<?>> _readSet = new HashMap<>();

    public ReadTrackingObjectSource(TransactionObjectSource delegate) {
        _delegate = delegate;
    }

    public Map<JObjectKey, TxReadObject<?>> getRead() {
        return Collections.unmodifiableMap(_readSet);
    }

    @Override
    public <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key) {
        var got = _readSet.get(key);

        if (got == null) {
            var read = _delegate.get(type, key);
            if (read.isPresent()) {
                _readSet.put(key, new TxReadObjectSome<>(read.get()));
            } else {
                _readSet.put(key, new TxReadObjectNone<>());
            }
            return read;
        }

        return switch (got) {
            case TxReadObjectNone<?> none -> Optional.empty();
            case TxReadObjectSome<?> some -> {
                if (type.isInstance(some.obj().data())) {
                    yield Optional.of((TransactionObject<T>) some.obj());
                } else {
                    yield Optional.empty();
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + got);
        };
    }

    @Override
    public <T extends JData> Optional<TransactionObject<T>> getWriteLocked(Class<T> type, JObjectKey key) {
        var got = _readSet.get(key);

        if (got == null) {
            var read = _delegate.getWriteLocked(type, key);
            if (read.isPresent()) {
                _readSet.put(key, new TxReadObjectSome<>(read.get()));
            } else {
                _readSet.put(key, new TxReadObjectNone<>());
            }
            return read;
        }

        return switch (got) {
            case TxReadObjectNone<?> none -> Optional.empty();
            case TxReadObjectSome<?> some -> {
                if (type.isInstance(some.obj().data())) {
                    yield Optional.of((TransactionObject<T>) some.obj());
                } else {
                    yield Optional.empty();
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + got);
        };
    }
}
