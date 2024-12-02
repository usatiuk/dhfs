package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.ObjectAllocator;
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

            var read = _source.get(type, key).orElse(null);

            if (read == null) {
                return Optional.empty();
            }

            switch (strategy) {
                case READ_ONLY: {
                    read.lock().readLock().lock();
                    var view = objectAllocator.unmodifiable(read.data());
                    _objects.put(key, new TxRecord.TxObjectRecordRead<>(read, view));
                    return Optional.of(view);
                }
                case OPTIMISTIC: {
                    var copy = objectAllocator.copy(read.data());
                    _objects.put(key, new TxRecord.TxObjectRecordCopyNoLock<>(read.data(), copy));
                    return Optional.of(copy.wrapped());
                }
                case WRITE: {
                    read.lock().writeLock().lock();
                    while (true) {
                        try {
                            var readAgain = _source.get(type, key).orElse(null);
                            if (readAgain == null) {
                                read.lock().writeLock().unlock();
                                return Optional.empty();
                            }
                            if (!Objects.equals(read, readAgain)) {
                                read.lock().writeLock().unlock();
                                read = readAgain;
                                read.lock().writeLock().lock();
                                continue;
                            }
                            var copy = objectAllocator.copy(read.data());
                            _objects.put(key, new TxRecord.TxObjectRecordCopyLock<>(read, copy));
                            return Optional.of(copy.wrapped());
                        } catch (Throwable e) {
                            read.lock().writeLock().unlock();
                            throw e;
                        }
                    }
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
