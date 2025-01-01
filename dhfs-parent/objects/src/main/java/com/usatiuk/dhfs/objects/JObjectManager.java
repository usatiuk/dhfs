package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.ObjectPersistentStore;
import com.usatiuk.dhfs.objects.persistence.TxManifest;
import com.usatiuk.dhfs.objects.transaction.*;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.dhfs.utils.DataLocker;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.io.Serializable;
import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

// Manages all access to com.usatiuk.objects.common.runtime.JData objects.
// In particular, it serves as a source of truth for what is committed to the backing storage.
// All data goes through it, it is responsible for transaction atomicity
// TODO: persistent tx id
@ApplicationScoped
public class JObjectManager {
    @Inject
    ObjectPersistentStore objectStorage;
    @Inject
    ObjectSerializer<JDataVersionedWrapper> objectSerializer;
    @Inject
    TransactionFactory transactionFactory;

    private final List<PreCommitTxHook> _preCommitTxHooks;

    JObjectManager(Instance<PreCommitTxHook> preCommitTxHooks) {
        _preCommitTxHooks = preCommitTxHooks.stream().sorted(Comparator.comparingInt(PreCommitTxHook::getPriority)).toList();
    }

    private final DataLocker _objLocker = new DataLocker();
    private final ConcurrentHashMap<JObjectKey, JDataWrapper<?>> _objects = new ConcurrentHashMap<>();
    private final AtomicLong _txCounter = new AtomicLong();

    private class JDataWrapper<T extends JData> extends WeakReference<JDataVersionedWrapper<T>> {
        private static final Cleaner CLEANER = Cleaner.create();

        public JDataWrapper(JDataVersionedWrapper<T> referent) {
            super(referent);
            var key = referent.data().key();
            CLEANER.register(referent, () -> {
                _objects.remove(key, this);
            });
        }

        @Override
        public String toString() {
            return "JDataWrapper{" +
                    "ref=" + get() +
                    '}';
        }
    }

    private <T extends JData> JDataVersionedWrapper<T> get(Class<T> type, JObjectKey key) {
        while (true) {
            {
                var got = _objects.get(key);

                if (got != null) {
                    var ref = got.get();
                    if (ref == null) {
                        _objects.remove(key, got);
                    } else if (type.isInstance(ref.data())) {
                        return (JDataVersionedWrapper<T>) ref;
                    } else {
                        throw new IllegalArgumentException("Object type mismatch: " + ref.getClass() + " vs " + type);
                    }
                }
            }

            //noinspection unused
            try (var readLock = _objLocker.lock(key)) {
                if (_objects.containsKey(key)) continue;

                var read = objectStorage.readObject(key)
                        .map(objectSerializer::deserialize)
                        .orElse(null);

                if (read == null) return null;

                if (type.isInstance(read.data())) {
                    var wrapper = new JDataWrapper<>((JDataVersionedWrapper<T>) read);
                    var old = _objects.put(key, wrapper);
                    assert old == null;
                    return read;
                } else {
                    throw new IllegalArgumentException("Object type mismatch: " + read.getClass() + " vs " + type);
                }
            }
        }
    }

    private record TransactionObjectNoLock<T extends JData>
            (Optional<JDataVersionedWrapper<T>> data)
            implements TransactionObject<T> {
    }

    private record TransactionObjectLocked<T extends JData>
            (Optional<JDataVersionedWrapper<T>> data, AutoCloseableNoThrow lock)
            implements TransactionObject<T> {
    }

    private <T extends JData> TransactionObjectNoLock<T> getObj(Class<T> type, JObjectKey key) {
        var got = get(type, key);
        return new TransactionObjectNoLock<>(Optional.ofNullable(got));
    }

    private <T extends JData> TransactionObjectLocked<T> getObjLock(Class<T> type, JObjectKey key) {
        var lock = _objLocker.lock(key);
        var got = get(type, key);
        return new TransactionObjectLocked<>(Optional.ofNullable(got), lock);
    }

    private class TransactionObjectSourceImpl implements TransactionObjectSource {
        private final long _txId;

        private TransactionObjectSourceImpl(long txId) {
            _txId = txId;
        }

        @Override
        public <T extends JData> TransactionObject<T> get(Class<T> type, JObjectKey key) {
            return getObj(type, key);
//            return getObj(type, key).map(got -> {
//                if (got.data().getVersion() > _txId) {
//                    throw new IllegalStateException("Serialization race for " + key + ": " + got.data().getVersion() + " vs " + _txId);
//                }
//                return got;
//            });
        }

        @Override
        public <T extends JData> TransactionObject<T> getWriteLocked(Class<T> type, JObjectKey key) {
            return getObjLock(type, key);
//            return getObjLock(type, key).map(got -> {
//                if (got.data().getVersion() > _txId) {
//                    got.lock.close();
//                    throw new IllegalStateException("Serialization race for " + key + ": " + got.data().getVersion() + " vs " + _txId);
//                }
//                return got;
//            });
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
        Log.trace("Committing transaction " + tx.getId());

        var current = new LinkedHashMap<JObjectKey, TxRecord.TxObjectRecord<?>>();
        var dependenciesLocked = new LinkedHashMap<JObjectKey, TransactionObjectLocked<?>>();
        Map<JObjectKey, TransactionObject<?>> reads;
        var toUnlock = new ArrayList<AutoCloseableNoThrow>();

        Consumer<JObjectKey> addDependency =
                key -> {
                    dependenciesLocked.computeIfAbsent(key, k -> {
                        Log.trace("Adding dependency " + k.toString());
                        var got = getObjLock(JData.class, k);
                        toUnlock.add(got.lock);
                        return got;
                    });
                };

        Function<JObjectKey, JData> getCurrent =
                key -> switch (current.get(key)) {
                    case TxRecord.TxObjectRecordWrite<?> write -> write.data();
                    case TxRecord.TxObjectRecordDeleted deleted -> null;
                    case null -> {
                        var dep = dependenciesLocked.get(key);
                        if (dep == null) {
                            throw new IllegalStateException("No dependency for " + key);
                        }
                        yield dep.data.map(JDataVersionedWrapper::data).orElse(null);
                    }
                    default -> {
                        throw new IllegalStateException("Unexpected value: " + current.get(key));
                    }
                };

        // For existing objects:
        // Check that their version is not higher than the version of transaction being committed
        // TODO: check deletions, inserts
        try {
            Collection<TxRecord.TxObjectRecord<?>> drained;
            {
                boolean somethingChanged;
                do {
                    somethingChanged = false;
                    for (var hook : _preCommitTxHooks) {
                        drained = tx.drainNewWrites();
                        Log.trace("Commit iteration with " + drained.size() + " records for hook " + hook.getClass());

                        drained.stream()
                                .map(TxRecord.TxObjectRecord::key)
                                .sorted(Comparator.comparing(JObjectKey::toString))
                                .forEach(addDependency);

                        for (var entry : drained) {
                            somethingChanged = true;
                            Log.trace("Running pre-commit hook " + hook.getClass() + " for" + entry.toString());
                            var oldObj = getCurrent.apply(entry.key());
                            switch (entry) {
                                case TxRecord.TxObjectRecordWrite<?> write -> {
                                    if (oldObj == null) {
                                        hook.onCreate(write.key(), write.data());
                                    } else {
                                        hook.onChange(write.key(), oldObj, write.data());
                                    }
                                }
                                case TxRecord.TxObjectRecordDeleted deleted -> {
                                    hook.onDelete(deleted.key(), oldObj);
                                }
                                default -> throw new IllegalStateException("Unexpected value: " + entry);
                            }
                            current.put(entry.key(), entry);
                        }
                    }
                } while (somethingChanged);
            }
            reads = tx.reads();
            for (var read : reads.entrySet()) {
                addDependency.accept(read.getKey());
                if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                    toUnlock.add(locked.lock);
                }
            }

            for (var dep : dependenciesLocked.entrySet()) {
                if (dep.getValue().data().isEmpty()) {
                    Log.trace("Checking dependency " + dep.getKey() + " - not found");
                    continue;
                }

                if (dep.getValue().data().get().version() >= tx.getId()) {
                    Log.trace("Checking dependency " + dep.getKey() + " - newer than");
                    throw new IllegalStateException("Serialization hazard: " + dep.getValue().data().get().version() + " vs " + tx.getId());
                }

                var read = reads.get(dep.getKey());
                if (read != null && read.data().orElse(null) != dep.getValue().data().orElse(null)) {
                    Log.trace("Checking dependency " + dep.getKey() + " - read mismatch");
                    throw new IllegalStateException("Read mismatch for " + dep.getKey() + ": " + read + " vs " + dep.getValue());
                }

                Log.trace("Checking dependency " + dep.getKey() + " - ok");
            }

            Log.tracef("Flushing transaction %d to storage", tx.getId());

            var toDelete = new ArrayList<JObjectKey>();
            var toWrite = new ArrayList<JObjectKey>();

            for (var action : current.entrySet()) {
                switch (action.getValue()) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
                        Log.trace("Flushing object " + action.getKey());
                        toWrite.add(action.getKey());
                        var wrapped = new JDataVersionedWrapper<>(write.data(), tx.getId());
                        var data = objectSerializer.serialize(wrapped);
                        objectStorage.writeObject(action.getKey(), data);
                        _objects.put(action.getKey(), new JDataWrapper<>(wrapped));
                    }
                    case TxRecord.TxObjectRecordDeleted deleted -> {
                        Log.trace("Deleting object " + action.getKey());
                        toDelete.add(action.getKey());
                        _objects.remove(action.getKey());
                    }
                    default -> {
                        throw new IllegalStateException("Unexpected value: " + action.getValue());
                    }
                }
            }

            Log.tracef("Committing transaction %d to storage", tx.getId());

            objectStorage.commitTx(new SimpleTxManifest(toWrite, toDelete));
        } catch (
                Throwable t) {
            Log.error("Error when committing transaction", t);
            throw t;
        } finally {
            for (var unlock : toUnlock) {
                unlock.close();
            }
        }
    }

    public void rollback(TransactionPrivate tx) {
    }
}