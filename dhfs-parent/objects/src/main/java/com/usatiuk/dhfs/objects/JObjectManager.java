package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.ObjectPersistentStore;
import com.usatiuk.dhfs.objects.persistence.TxManifest;
import com.usatiuk.dhfs.objects.transaction.TransactionFactory;
import com.usatiuk.dhfs.objects.transaction.TransactionObjectSource;
import com.usatiuk.dhfs.objects.transaction.TransactionPrivate;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import com.usatiuk.dhfs.utils.DataLocker;
import com.usatiuk.dhfs.utils.VoidFn;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Manages all access to com.usatiuk.objects.common.runtime.JData objects.
// In particular, it serves as a source of truth for what is committed to the backing storage.
// All data goes through it, it is responsible for transaction atomicity
@ApplicationScoped
public class JObjectManager {
    @Inject
    ObjectPersistentStore objectStorage;
    @Inject
    ObjectSerializer<JData> objectSerializer;
    @Inject
    ObjectAllocator objectAllocator;
    @Inject
    TransactionFactory transactionFactory;

    private final DataLocker _storageReadLocker = new DataLocker();
    private final ConcurrentHashMap<JObjectKey, JDataWrapper<?>> _objects = new ConcurrentHashMap<>();
    private final AtomicLong _txCounter = new AtomicLong();

    private class JDataWrapper<T extends JData> extends WeakReference<T> {
        private static final Cleaner CLEANER = Cleaner.create();

        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        long lastWriteTx = 0;

        public JDataWrapper(T referent) {
            super(referent);
            var key = referent.getKey();
            CLEANER.register(referent, () -> {
                _objects.remove(key, this);
            });
        }
    }

    private <T extends JData> Pair<T, JDataWrapper<T>> get(Class<T> type, JObjectKey key) {
        while (true) {
            {
                var got = _objects.get(key);

                if (got != null) {
                    var ref = got.get();
                    if (type.isInstance(ref)) {
                        return Pair.of(type.cast(ref), (JDataWrapper<T>) got);
                    } else if (ref == null) {
                        _objects.remove(key, got);
                    } else {
                        throw new IllegalArgumentException("Object type mismatch");
                    }
                }
            }

            //noinspection unused
            try (var readLock = _storageReadLocker.lock(key)) {
                var read = objectStorage.readObject(key).orElse(null);
                if (read == null) throw new IllegalArgumentException("Object not found");

                var got = objectSerializer.deserialize(read);

                if (type.isInstance(got)) {
                    var wrapper = new JDataWrapper<T>((T) got);
                    var old = _objects.putIfAbsent(key, wrapper);
                    if (old != null) continue;
                    return Pair.of(type.cast(got), wrapper);
                } else if (got == null) {
                    return null;
                } else {
                    throw new IllegalArgumentException("Object type mismatch");
                }
            }
        }
    }

    private record TransactionObjectImpl<T extends JData>
            (T data, ReadWriteLock lock)
            implements TransactionObjectSource.TransactionObject<T> {}

    private final TransactionObjectSource _objSource = new TransactionObjectSource() {
        @Override
        public <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key) {
            var got = JObjectManager.this.get(type, key);
            if (got == null) return Optional.empty();
            return Optional.of(new TransactionObjectImpl<>(got.getLeft(), got.getRight().lock));
        }
    };

    public TransactionPrivate createTransaction() {
        var counter = _txCounter.getAndIncrement();
        Log.trace("Creating transaction " + counter);
        return transactionFactory.createTransaction(counter, _objSource);
    }


    public void commit(TransactionPrivate tx) {
        var toUnlock = new LinkedList<VoidFn>();
        var toFlush = new LinkedList<TxRecord.TxObjectRecordWrite<?>>();
        var toLock = new ArrayList<TxRecord.TxObjectRecordCopyNoLock<?>>();

        Log.trace("Committing transaction " + tx.getId());

        try {
            for (var entry : tx.drain()) {
                Log.trace("Processing entry " + entry.toString());
                switch (entry) {
                    case TxRecord.TxObjectRecordRead<?> read -> {
                        toUnlock.add(read.original().lock().readLock()::unlock);
                    }
                    case TxRecord.TxObjectRecordCopyLock<?> copy -> {
                        toUnlock.add(copy.original().lock().writeLock()::unlock);
                        if (copy.copy().isModified()) {
                            toFlush.add(copy);
                        }
                    }
                    case TxRecord.TxObjectRecordCopyNoLock<?> copy -> {
                        if (copy.copy().isModified()) {
                            toLock.add(copy);
                            toFlush.add(copy);
                        }
                    }
                    case TxRecord.TxObjectRecordNew<?> created -> {
                        toFlush.add(created);
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + entry);
                }
            }

            toLock.sort(Comparator.comparingInt(a -> System.identityHashCode(a.original())));

            for (var record : toLock) {
                Log.trace("Locking " + record.toString());

                var found = _objects.get(record.original().getKey());

                if (found.get() != record.original()) {
                    throw new IllegalStateException("Object changed during transaction");
                }

                found.lock.writeLock().lock();
                toUnlock.add(found.lock.writeLock()::unlock);
            }

            for (var record : toFlush) {
                Log.trace("Processing flush entry " + record.toString());

                var current = _objects.get(record.copy().wrapped().getKey());

                if (current == null && !(record instanceof TxRecord.TxObjectRecordNew<?>)) {
                    throw new IllegalStateException("Object not found during transaction");
                } else if (current != null) {
                    var old = switch (record) {
                        case TxRecord.TxObjectRecordCopyLock<?> copy -> copy.original().data();
                        case TxRecord.TxObjectRecordCopyNoLock<?> copy -> copy.original();
                        default -> throw new IllegalStateException("Unexpected value: " + record);
                    };

                    if (current.get() != old) {
                        throw new IllegalStateException("Object changed during transaction");
                    }

                    if (current.lastWriteTx > tx.getId()) {
                        throw new IllegalStateException("Transaction race");
                    }

                    var newWrapper = new JDataWrapper<>(record.copy().wrapped());
                    newWrapper.lock.writeLock().lock();
                    if (!_objects.replace(record.copy().wrapped().getKey(), current, newWrapper)) {
                        throw new IllegalStateException("Object changed during transaction");
                    }
                    toUnlock.add(newWrapper.lock.writeLock()::unlock);
                } else if (record instanceof TxRecord.TxObjectRecordNew<?> created) {
                    var wrapper = new JDataWrapper<>(created.created());
                    wrapper.lock.writeLock().lock();
                    var old = _objects.putIfAbsent(created.created().getKey(), wrapper);
                    if (old != null)
                        throw new IllegalStateException("Object already exists");
                    toUnlock.add(wrapper.lock.writeLock()::unlock);
                } else {
                    throw new IllegalStateException("Object not found during transaction");
                }
            }

            // Have all locks now
            for (var record : toFlush) {
                Log.trace("Flushing " + record.toString());

                assert record.copy().isModified();

                var obj = record.copy().wrapped();
                var key = obj.getKey();
                var data = objectSerializer.serialize(obj);
                objectStorage.writeObject(key, data);
                _objects.get(key).lastWriteTx = tx.getId(); // FIXME:
            }

            Log.trace("Flushing transaction " + tx.getId());

            objectStorage.commitTx(new TxManifest() {
                @Override
                public List<JObjectKey> getWritten() {
                    return toFlush.stream().map(r -> r.copy().wrapped().getKey()).toList();
                }

                @Override
                public List<JObjectKey> getDeleted() {
                    return List.of();
                }
            });
        } catch (Throwable t) {
            Log.error("Error when committing transaction", t);
            throw t;
        } finally {
            for (var unlock : toUnlock) {
                unlock.apply();
            }
        }
    }
}