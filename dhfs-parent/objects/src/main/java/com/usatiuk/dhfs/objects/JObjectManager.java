package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.ObjectPersistentStore;
import com.usatiuk.dhfs.objects.transaction.TransactionFactory;
import com.usatiuk.dhfs.objects.transaction.TransactionObjectSource;
import com.usatiuk.dhfs.objects.transaction.TransactionPrivate;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import com.usatiuk.dhfs.utils.DataLocker;
import com.usatiuk.dhfs.utils.VoidFn;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Manages all access to JData objects.
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

    private final TransactionObjectSource _objSource = new TransactionObjectSource() {
        @Override
        public <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key) {
            var got = JObjectManager.this.get(type, key);
            if (got == null) return Optional.empty();
            return Optional.of(new TransactionObject<>() {
                @Override
                public T get() {
                    return got.getLeft();
                }

                @Override
                public ReadWriteLock getLock() {
                    return got.getRight().lock;
                }
            });

        }
    };

    public TransactionPrivate createTransaction() {
        return transactionFactory.createTransaction(_txCounter.getAndIncrement(), _objSource);
    }


    public void commit(TransactionPrivate tx) {
        var toUnlock = new LinkedList<VoidFn>();
        var toFlush = new LinkedList<TxRecord.TxObjectRecordWrite<?>>();
        var toLock = new ArrayList<TxRecord.TxObjectRecordCopyNoLock<?>>();

        try {
            for (var entry : tx.drain()) {
                switch (entry) {
                    case TxRecord.TxObjectRecordRead<?> read -> {
                        toUnlock.add(read.original().getLock().readLock()::unlock);
                    }
                    case TxRecord.TxObjectRecordCopyLock<?> copy -> {
                        toUnlock.add(copy.original().getLock().writeLock()::unlock);
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

            for (var record : toLock) {
                var found = _objects.get(record.original().getKey());

                if (found.get() != record.original()) {
                    throw new IllegalStateException("Object changed during transaction");
                }

                found.lock.writeLock().lock();
                toUnlock.add(found.lock.writeLock()::unlock);
            }

            for (var record : toFlush) {
                var current = _objects.get(record.copy().wrapped().getKey());


                assert current == null && record instanceof TxRecord.TxObjectRecordNew<?> || current == record.copy().wrapped();

                if (current.get() != )

            }

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