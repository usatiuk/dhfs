package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.*;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.dhfs.utils.DataLocker;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

// Manages all access to com.usatiuk.dhfs.objects.JData objects.
// In particular, it serves as a source of truth for what is committed to the backing storage.
// All data goes through it, it is responsible for transaction atomicity
// TODO: persistent tx id
@ApplicationScoped
public class JObjectManager {
    private final List<PreCommitTxHook> _preCommitTxHooks;
    private final DataLocker _objLocker = new DataLocker();
    private final ConcurrentHashMap<JObjectKey, JDataWrapper<?>> _objects = new ConcurrentHashMap<>();
    private final AtomicLong _txCounter = new AtomicLong();
    @Inject
    WritebackObjectPersistentStore writebackObjectPersistentStore;
    @Inject
    TransactionFactory transactionFactory;
    JObjectManager(Instance<PreCommitTxHook> preCommitTxHooks) {
        _preCommitTxHooks = preCommitTxHooks.stream().sorted(Comparator.comparingInt(PreCommitTxHook::getPriority)).toList();
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
                        throw new IllegalArgumentException("Object type mismatch: " + ref.data().getClass() + " vs " + type);
                    }
                }
            }

            //noinspection unused
            try (var readLock = _objLocker.lock(key)) {
                if (_objects.containsKey(key)) continue;

                var read = writebackObjectPersistentStore.readObject(key).orElse(null);

                if (read == null) return null;

                if (type.isInstance(read.data())) {
                    var wrapper = new JDataWrapper<>((JDataVersionedWrapper<T>) read);
                    var old = _objects.put(key, wrapper);
                    assert old == null;
                    return (JDataVersionedWrapper<T>) read;
                } else {
                    throw new IllegalArgumentException("Object type mismatch: " + read.getClass() + " vs " + type);
                }
            }
        }
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

    public TransactionPrivate createTransaction() {
        var counter = _txCounter.getAndIncrement();
        Log.trace("Creating transaction " + counter);
        return transactionFactory.createTransaction(counter, new TransactionObjectSourceImpl(counter));
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
                        var got = getObjLock(JData.class, k);
                        Log.trace("Adding dependency " + k.toString() + " -> " + got);
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
                            throw new TxCommitException("No dependency for " + key);
                        }
                        yield dep.data.map(JDataVersionedWrapper::data).orElse(null);
                    }
                    default -> {
                        throw new TxCommitException("Unexpected value: " + current.get(key));
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
                                default -> throw new TxCommitException("Unexpected value: " + entry);
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
                    throw new TxCommitException("Serialization hazard: " + dep.getValue().data().get().version() + " vs " + tx.getId());
                }

                var read = reads.get(dep.getKey());
                if (read != null && read.data().orElse(null) != dep.getValue().data().orElse(null)) {
                    Log.trace("Checking dependency " + dep.getKey() + " - read mismatch");
                    throw new TxCommitException("Read mismatch for " + dep.getKey() + ": " + read + " vs " + dep.getValue());
                }

                Log.trace("Checking dependency " + dep.getKey() + " - ok with read " + read);
            }

            Log.tracef("Flushing transaction %d to storage", tx.getId());

            for (var action : current.entrySet()) {
                switch (action.getValue()) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
                        Log.trace("Flushing object " + action.getKey());
                        var wrapped = new JDataVersionedWrapper<>(write.data(), tx.getId());
                        _objects.put(action.getKey(), new JDataWrapper<>(wrapped));
                    }
                    case TxRecord.TxObjectRecordDeleted deleted -> {
                        Log.trace("Deleting object " + action.getKey());
                        _objects.remove(action.getKey());
                    }
                    default -> {
                        throw new TxCommitException("Unexpected value: " + action.getValue());
                    }
                }
            }

            Log.tracef("Committing transaction %d to storage", tx.getId());
            writebackObjectPersistentStore.commitTx(current.values(), tx.getId());
        } catch (Throwable t) {
            Log.trace("Error when committing transaction", t);
            throw new TxCommitException(t.getMessage(), t);
        } finally {
            for (var unlock : toUnlock) {
                unlock.close();
            }
        }
    }

    public void rollback(TransactionPrivate tx) {
        Log.trace("Rolling back transaction " + tx.getId());
        tx.reads().forEach((key, value) -> {
            if (value instanceof TransactionObjectLocked<?> locked) {
                locked.lock.close();
            }
        });
    }

    private record TransactionObjectNoLock<T extends JData>
            (Optional<JDataVersionedWrapper<T>> data)
            implements TransactionObject<T> {
    }

    private record TransactionObjectLocked<T extends JData>
            (Optional<JDataVersionedWrapper<T>> data, AutoCloseableNoThrow lock)
            implements TransactionObject<T> {
    }

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
}