package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class TransactionFactoryImpl implements TransactionFactory {
    @Inject
    ObjectAllocator objectAllocator;

    private class TransactionImpl implements TransactionPrivate {
        @Getter(AccessLevel.PUBLIC)
        private final long _id;
        private final ReadTrackingObjectSource _source;

        private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _objects = new HashMap<>();
        private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _newObjects = new HashMap<>();

        private TransactionImpl(long id, TransactionObjectSource source) {
            _id = id;
            _source = new ReadTrackingObjectSource(source);
        }

        @Override
        public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
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
                    _newObjects.put(key, new TxRecord.TxObjectRecordOptimistic<>(read, copy));
                    return Optional.of(copy.wrapped());
                }
                case WRITE: {
                    var locked = _source.getWriteLocked(type, key).orElse(null);
                    if (locked == null) {
                        return Optional.empty();
                    }
                    var copy = objectAllocator.copy(locked.data());
                    _objects.put(key, new TxRecord.TxObjectRecordCopyLock<>(locked, copy));
                    _newObjects.put(key, new TxRecord.TxObjectRecordCopyLock<>(locked, copy));
                    return Optional.of(copy.wrapped());
                }
                default:
                    throw new IllegalArgumentException("Unknown locking strategy");
            }
        }

        @Override
        public void delete(JObjectKey key) {
            // FIXME
            var got = _objects.get(key);
            if (got != null) {
                switch (got) {
                    case TxRecord.TxObjectRecordNew<?> created -> {
                        _objects.remove(key);
                        _newObjects.remove(key);
                    }
                    case TxRecord.TxObjectRecordCopyLock<?> copyLockRecord -> {
                        _objects.put(key, new TxRecord.TxObjectRecordDeleted<>(copyLockRecord.original()));
                        _newObjects.put(key, new TxRecord.TxObjectRecordDeleted<>(copyLockRecord.original()));
                    }
                    case TxRecord.TxObjectRecordOptimistic<?> optimisticRecord -> {
                        _objects.put(key, new TxRecord.TxObjectRecordDeleted<>(optimisticRecord.original()));
                        _newObjects.put(key, new TxRecord.TxObjectRecordDeleted<>(optimisticRecord.original()));
                    }
                    case TxRecord.TxObjectRecordDeleted<?> deletedRecord -> {
                        return;
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + got);
                }
            }

            var read = _source.get(JData.class, key).orElse(null);
            if (read == null) {
                return;
            }
            _objects.put(key, new TxRecord.TxObjectRecordDeleted<>(read));
            _newObjects.put(key, new TxRecord.TxObjectRecordDeleted<>(read));
        }

        @Override
        public void put(JData obj) {
            if (_objects.containsKey(obj.getKey())) {
                throw new IllegalArgumentException("Object already exists in transaction");
            }

            _objects.put(obj.getKey(), new TxRecord.TxObjectRecordNew<>(obj));
            _newObjects.put(obj.getKey(), new TxRecord.TxObjectRecordNew<>(obj));
        }

        @Override
        public Collection<TxRecord.TxObjectRecord<?>> drainNewWrites() {
            var ret = _newObjects;
            _newObjects = new HashMap<>();
            return ret.values();
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
