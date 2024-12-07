package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.*;

@ApplicationScoped
public class TransactionFactoryImpl implements TransactionFactory {
    @Inject
    ObjectAllocator objectAllocator;

    private class TransactionImpl implements TransactionPrivate {
        @Getter(AccessLevel.PUBLIC)
        private final long _id;
        private final ReadTrackingObjectSource _source;

        private final Map<JObjectKey, TxRecord.TxObjectRecord<?>> _objects = new HashMap<>();

        private TransactionImpl(long id, TransactionObjectSource source) {
            _id = id;
            _source = new ReadTrackingObjectSource(source);
        }

        @Override
        public <T extends JData> Optional<T> getObject(Class<T> type, JObjectKey key, LockingStrategy strategy) {
            var got = _objects.get(key);
            if (got != null) {
                var compatible = got.getIfStrategyCompatible(key, strategy);
                if (compatible == null) {
                    throw new IllegalArgumentException("Locking strategy mismatch");
                }
                if (!type.isInstance(compatible)) {
                    throw new IllegalArgumentException("Object type mismatch");
                }
                return Optional.of(type.cast(compatible));
            }

            switch (strategy) {
                case OPTIMISTIC: {
                    var read = _source.get(type, key).orElse(null);
                    if (read == null) {
                        return Optional.empty();
                    }
                    var copy = objectAllocator.copy(read.data());
                    _objects.put(key, new TxRecord.TxObjectRecordOptimistic<>(read, copy));
                    return Optional.of(copy.wrapped());
                }
                case WRITE: {
                    var locked = _source.getWriteLocked(type, key).orElse(null);
                    if (locked == null) {
                        return Optional.empty();
                    }
                    var copy = objectAllocator.copy(locked.data());
                    _objects.put(key, new TxRecord.TxObjectRecordCopyLock<>(locked, copy));
                    return Optional.of(copy.wrapped());
                }
                default:
                    throw new IllegalArgumentException("Unknown locking strategy");
            }
        }

        @Override
        public void putObject(JData obj) {
            if (_objects.containsKey(obj.getKey())) {
                throw new IllegalArgumentException("Object already exists in transaction");
            }

            _objects.put(obj.getKey(), new TxRecord.TxObjectRecordNew<>(obj));
        }

        @Override
        public Collection<TxRecord.TxObjectRecord<?>> writes() {
            return Collections.unmodifiableCollection(_objects.values());
        }

        @Override
        public Map<JObjectKey, ReadTrackingObjectSource.TxReadObject<?>> reads() {
            return _source.getRead();
        }
    }

    @Override
    public TransactionPrivate createTransaction(long id, TransactionObjectSource source) {
        return new TransactionImpl(id, source);
    }

}
