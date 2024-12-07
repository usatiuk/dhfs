package com.usatiuk.dhfs.objects;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.objects.persistence.ObjectPersistentStore;
import com.usatiuk.dhfs.objects.persistence.TxManifest;
import com.usatiuk.dhfs.objects.transaction.*;
import com.usatiuk.dhfs.utils.DataLocker;
import com.usatiuk.dhfs.utils.VoidFn;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.Serializable;
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
// TODO: persistent tx id
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

        public JDataWrapper(T referent) {
            super(referent);
            var key = referent.getKey();
            CLEANER.register(referent, () -> {
                _objects.remove(key, this);
            });
        }

        @Override
        public String toString() {
            return "JDataWrapper{" +
                    "ref=" + get() +
                    ", lock=" + lock +
                    '}';
        }
    }

    private record WrapperRet<T extends JData>(T obj, JDataWrapper<T> wrapper) {
    }

    private <T extends JData> WrapperRet<T> get(Class<T> type, JObjectKey key) {
        while (true) {
            {
                var got = _objects.get(key);

                if (got != null) {
                    var ref = got.get();
                    if (type.isInstance(ref)) {
                        return new WrapperRet<>((T) ref, (JDataWrapper<T>) got);
                    } else if (ref == null) {
                        _objects.remove(key, got);
                    } else {
                        throw new IllegalArgumentException("Object type mismatch: " + ref.getClass() + " vs " + type);
                    }
                }
            }

            //noinspection unused
            try (var readLock = _storageReadLocker.lock(key)) {
                var read = objectStorage.readObject(key).orElse(null);
                if (read == null) return null;

                var got = objectSerializer.deserialize(read);

                if (type.isInstance(got)) {
                    var wrapper = new JDataWrapper<>((T) got);
                    var old = _objects.putIfAbsent(key, wrapper);
                    if (old != null) continue;
                    return new WrapperRet<>((T) got, wrapper);
                } else if (got == null) {
                    return null;
                } else {
                    throw new IllegalArgumentException("Object type mismatch: " + got.getClass() + " vs " + type);
                }
            }
        }
    }


    private <T extends JData> WrapperRet<T> getLocked(Class<T> type, JObjectKey key, boolean write) {
        var read = get(type, key);
        if (read == null) return null;
        var lock = write ? read.wrapper().lock.writeLock() : read.wrapper().lock.readLock();
        lock.lock();
        while (true) {
            try {
                var readAgain = get(type, key);
                if (readAgain == null) {
                    lock.unlock();
                    return null;
                }
                if (!Objects.equals(read, readAgain)) {
                    lock.unlock();
                    read = readAgain;
                    lock = write ? read.wrapper().lock.writeLock() : read.wrapper().lock.readLock();
                    lock.lock();
                    continue;
                }
                return read;
            } catch (Throwable e) {
                lock.unlock();
                throw e;
            }
        }
    }

    private record TransactionObjectImpl<T extends JData>
            (T data, ReadWriteLock lock)
            implements TransactionObject<T> {
    }

    private class TransactionObjectSourceImpl implements TransactionObjectSource {
        private final long _txId;

        private TransactionObjectSourceImpl(long txId) {
            _txId = txId;
        }

        @Override
        public <T extends JData> Optional<TransactionObject<T>> get(Class<T> type, JObjectKey key) {
            var got = JObjectManager.this.get(type, key);
            if (got == null) return Optional.empty();
            return Optional.of(new TransactionObjectImpl<>(got.obj(), got.wrapper().lock));
        }

        @Override
        public <T extends JData> Optional<TransactionObject<T>> getWriteLocked(Class<T> type, JObjectKey key) {
            var got = JObjectManager.this.getLocked(type, key, true);
            if (got == null) return Optional.empty();
            if (got.obj.getVersion() >= _txId) {
                got.wrapper().lock.writeLock().unlock();
                throw new IllegalStateException("Serialization race");
            }
            return Optional.of(new TransactionObjectImpl<>(got.obj(), got.wrapper().lock));
        }
    }

    public TransactionPrivate createTransaction() {
        var counter = _txCounter.getAndIncrement();
        Log.trace("Creating transaction " + counter);
        return transactionFactory.createTransaction(counter, new TransactionObjectSourceImpl(counter));
    }

    // FIXME:
    private static class SimpleTxManifest implements Serializable, TxManifest {
        private final List<JObjectKey> _written;
        private final List<JObjectKey> _deleted;

        public SimpleTxManifest(List<JObjectKey> written, List<JObjectKey> deleted) {
            _written = written;
            _deleted = deleted;
        }

        @Override
        public List<JObjectKey> getWritten() {
            return _written;
        }

        @Override
        public List<JObjectKey> getDeleted() {
            return _deleted;
        }
    }

    public void commit(TransactionPrivate tx) {
        // This also holds the weak references
        var toUnlock = new LinkedList<VoidFn>();

        var toFlush = new LinkedList<TxRecord.TxObjectRecordWrite<?>>();
        var toPut = new LinkedList<TxRecord.TxObjectRecordNew<?>>();
        var toLock = new ArrayList<TransactionObject<?>>();
        var dependencies = new LinkedList<TransactionObject<?>>();

        Log.trace("Committing transaction " + tx.getId());

        // For existing objects:
        // Check that their version is not higher than the version of transaction being committed
        // TODO: check deletions, inserts

        try {
            for (var entry : tx.writes()) {
                Log.trace("Processing write " + entry.toString());
                switch (entry) {
                    case TxRecord.TxObjectRecordCopyLock<?> copy -> {
                        toUnlock.add(copy.original().lock().writeLock()::unlock);
                        toFlush.add(copy);
                    }
                    case TxRecord.TxObjectRecordOptimistic<?> copy -> {
                        toLock.add(copy.original());
                        toFlush.add(copy);
                    }
                    case TxRecord.TxObjectRecordNew<?> created -> {
                        toPut.add(created);
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + entry);
                }
            }

            for (var entry : tx.reads().entrySet()) {
                Log.trace("Processing read " + entry.toString());
                switch (entry.getValue()) {
                    case ReadTrackingObjectSource.TxReadObjectNone<?> none -> {
                        // TODO: Check this
                    }
                    case ReadTrackingObjectSource.TxReadObjectSome<?>(var obj) -> {
                        toLock.add(obj);
                        dependencies.add(obj);
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + entry);
                }
            }

            toLock.sort(Comparator.comparingInt(System::identityHashCode));

            for (var record : toLock) {
                Log.trace("Locking " + record.toString());

                var got = getLocked(record.data().getClass(), record.data().getKey(), true);

                if (got == null) {
                    throw new IllegalStateException("Object " + record.data().getKey() + " not found");
                }

                toUnlock.add(got.wrapper().lock.writeLock()::unlock);

                if (got.obj() != record.data()) {
                    throw new IllegalStateException("Object changed during transaction: " + got.obj() + " vs " + record.data());
                }
            }

            for (var dep : dependencies) {
                Log.trace("Checking dependency " + dep.toString());
                var current = _objects.get(dep.data().getKey()).get();

                // Checked above
                assert current == dep.data();

                if (current.getVersion() >= tx.getId()) {
                    throw new IllegalStateException("Serialization hazard: " + current.getVersion() + " vs " + tx.getId());
                }
            }

            for (var put : toPut) {
                Log.trace("Putting new object " + put.toString());
                var wrapper = new JDataWrapper<>(put.created());
                wrapper.lock.writeLock().lock();
                var old = _objects.putIfAbsent(put.created().getKey(), wrapper);
                if (old != null)
                    throw new IllegalStateException("Object already exists: " + old.get());
                toUnlock.add(wrapper.lock.writeLock()::unlock);
            }

            for (var record : toFlush) {
                if (!record.copy().isModified()) {
                    Log.trace("Not changed " + record.toString());
                    continue;
                }

                Log.trace("Flushing changed " + record.toString());
                var current = _objects.get(record.original().data().getKey());

                var newWrapper = new JDataWrapper<>(record.copy().wrapped());
                newWrapper.lock.writeLock().lock();
                if (!_objects.replace(record.copy().wrapped().getKey(), current, newWrapper)) {
                    assert false; // Should not happen, as the object is locked
                    throw new IllegalStateException("Object changed during transaction after locking: " + current.get() + " vs " + record.copy().wrapped());
                }
                toUnlock.add(newWrapper.lock.writeLock()::unlock);
            }

            Log.tracef("Flushing transaction %d to storage", tx.getId());

            var written = Streams.concat(toFlush.stream().map(f -> f.copy().wrapped()),
                    toPut.stream().map(TxRecord.TxObjectRecordNew::created)).toList();

            // Really flushing to storage
            written.forEach(obj -> {
                Log.trace("Flushing object " + obj.getKey());
                assert obj.getVersion() == tx.getId();
                var key = obj.getKey();
                var data = objectSerializer.serialize(obj);
                objectStorage.writeObject(key, data);
            });

            Log.tracef("Committing transaction %d to storage", tx.getId());

            objectStorage.commitTx(new SimpleTxManifest(written.stream().map(JData::getKey).toList(), Collections.emptyList()));
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