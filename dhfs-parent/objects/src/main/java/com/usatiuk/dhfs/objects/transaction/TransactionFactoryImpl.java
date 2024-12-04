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
        private final TransactionObjectSource _source;

        private final Map<JObjectKey, TxRecord.TxObjectRecord<?>> _objects = new HashMap<>();

        private TransactionImpl(long id, TransactionObjectSource source) {
            _id = id;
            _source = source;
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
                case READ:
                case READ_SERIALIZABLE: {
                    var locked = strategy == LockingStrategy.READ_SERIALIZABLE
                            ? _source.getReadLockedSerializable(type, key).orElse(null)
                            : _source.getReadLocked(type, key).orElse(null);
                    if (locked == null) {
                        return Optional.empty();
                    }
                    var view = objectAllocator.unmodifiable(locked.data());
                    _objects.put(key,
                            strategy == LockingStrategy.READ_SERIALIZABLE
                                    ? new TxRecord.TxObjectRecordReadSerializable<>(locked, view)
                                    : new TxRecord.TxObjectRecordRead<>(locked, view)
                    );
                    return Optional.of(view);
                }
                case OPTIMISTIC: {
                    var read = _source.get(type, key).orElse(null);

                    if (read == null) {
                        return Optional.empty();
                    }
                    var copy = objectAllocator.copy(read.data());
                    _objects.put(key, new TxRecord.TxObjectRecordCopyNoLock<>(read.data(), copy));
                    return Optional.of(copy.wrapped());
                }
                case WRITE:
                case WRITE_SERIALIZABLE: {
                    var locked = strategy == LockingStrategy.WRITE_SERIALIZABLE
                            ? _source.getWriteLockedSerializable(type, key).orElse(null)
                            : _source.getWriteLocked(type, key).orElse(null);
                    if (locked == null) {
                        return Optional.empty();
                    }
                    var copy = objectAllocator.copy(locked.data());
                    _objects.put(key,
                            strategy == LockingStrategy.WRITE_SERIALIZABLE
                                    ? new TxRecord.TxObjectRecordCopyLockSerializable<>(locked, copy)
                                    : new TxRecord.TxObjectRecordCopyLock<>(locked, copy)
                    );
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
        public Collection<TxRecord.TxObjectRecord<?>> drain() {
            return Collections.unmodifiableCollection(_objects.values());
        }
    }

    @Override
    public TransactionPrivate createTransaction(long id, TransactionObjectSource source) {
        return new TransactionImpl(id, source);
    }

}
