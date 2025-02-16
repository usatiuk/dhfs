package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.*;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.dhfs.utils.DataLocker;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

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
    private boolean _ready = false;
    @Inject
    WritebackObjectPersistentStore writebackObjectPersistentStore;
    @Inject
    TransactionFactory transactionFactory;

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    void init(@Observes @Priority(200) StartupEvent event) {
        var read = writebackObjectPersistentStore.readObject(JDataDummy.TX_ID_OBJ_NAME).orElse(null);
        if (read != null) {
            _txCounter.set(read.version());
        }
        _ready = true;
    }

    JObjectManager(Instance<PreCommitTxHook> preCommitTxHooks) {
        _preCommitTxHooks = preCommitTxHooks.stream().sorted(Comparator.comparingInt(PreCommitTxHook::getPriority)).toList();
    }

    private <T extends JData> JDataVersionedWrapper<T> get(Class<T> type, JObjectKey key) {
        verifyReady();
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
        verifyReady();
        var got = get(type, key);
        return new TransactionObjectNoLock<>(Optional.ofNullable(got));
    }

    private <T extends JData> TransactionObjectLocked<T> getObjLock(Class<T> type, JObjectKey key) {
        verifyReady();
        var lock = _objLocker.lock(key);
        var got = get(type, key);
        return new TransactionObjectLocked<>(Optional.ofNullable(got), lock);
    }

    public TransactionPrivate createTransaction() {
        verifyReady();
        var counter = _txCounter.getAndIncrement();
        Log.trace("Creating transaction " + counter);
        return transactionFactory.createTransaction(counter, new TransactionObjectSourceImpl(counter));
    }

    public TransactionHandle commit(TransactionPrivate tx) {
        verifyReady();
        Log.trace("Committing transaction " + tx.getId());
        // FIXME: Better way?
        tx.put(JDataDummy.getInstance());

        var current = new LinkedHashMap<JObjectKey, TxRecord.TxObjectRecord<?>>();
        var dependenciesLocked = new LinkedHashMap<JObjectKey, TransactionObjectLocked<?>>();
        Map<JObjectKey, TransactionObject<?>> reads;
        var toUnlock = new ArrayList<AutoCloseableNoThrow>();

        Consumer<JObjectKey> addDependency =
                key -> {
                    dependenciesLocked.computeIfAbsent(key, k -> {
                        var got = getObjLock(JData.class, k);
                        Log.trace("Adding dependency " + k.toString() + " -> " + got.data().map(JDataVersionedWrapper::data).map(JData::key).orElse(null));
                        toUnlock.add(got.lock);
                        return got;
                    });
                };

        // For existing objects:
        // Check that their version is not higher than the version of transaction being committed
        // TODO: check deletions, inserts
        try {
            try {
                Function<JObjectKey, JData> getCurrent =
                        key -> switch (current.get(key)) {
                            case TxRecord.TxObjectRecordWrite<?> write -> write.data();
                            case TxRecord.TxObjectRecordDeleted deleted -> null;
                            case null ->
                                    tx.readSource().get(JData.class, key).data().map(JDataVersionedWrapper::data).orElse(null);
                            default -> {
                                throw new TxCommitException("Unexpected value: " + current.get(key));
                            }
                        };

                boolean somethingChanged;
                do {
                    somethingChanged = false;
                    Map<JObjectKey, TxRecord.TxObjectRecord<?>> currentIteration = new HashMap();
                    for (var hook : _preCommitTxHooks) {
                        for (var n : tx.drainNewWrites())
                            currentIteration.put(n.key(), n);
                        Log.trace("Commit iteration with " + currentIteration.size() + " records for hook " + hook.getClass());

                        for (var entry : currentIteration.entrySet()) {
                            // FIXME: Kinda hack?
                            if (entry.getKey().equals(JDataDummy.TX_ID_OBJ_NAME)) {
                                continue;
                            }
                            somethingChanged = true;
                            Log.trace("Running pre-commit hook " + hook.getClass() + " for" + entry.getKey());
                            var oldObj = getCurrent.apply(entry.getKey());
                            switch (entry.getValue()) {
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
                        }
                    }
                    current.putAll(currentIteration);
                } while (somethingChanged);
            } finally {
                reads = tx.reads();

                Stream.concat(reads.keySet().stream(), current.keySet().stream())
                        .sorted(Comparator.comparing(JObjectKey::toString))
                        .forEach(addDependency);

                for (var read : reads.entrySet()) {
                    if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                        toUnlock.add(locked.lock);
                    }
                }
            }

            for (var read : reads.entrySet()) {
                var dep = dependenciesLocked.get(read.getKey());

                if (dep.data().isEmpty()) {
                    Log.trace("Checking read dependency " + read.getKey() + " - not found");
                    continue;
                }

                if (dep.data().orElse(null) != read.getValue().data().orElse(null)) {
                    Log.trace("Checking dependency " + read.getKey() + " - changed already");
                    throw new TxCommitException("Serialization hazard: " + dep.data().get().version() + " vs " + tx.getId());
                }

                if (dep.data().get().version() >= tx.getId()) {
                    assert false;
                    Log.trace("Checking dependency " + read.getKey() + " - newer than");
                    throw new TxCommitException("Serialization hazard: " + dep.data().get().version() + " vs " + tx.getId());
                }

                Log.trace("Checking dependency " + read.getKey() + " - ok with read");
            }

            Log.tracef("Flushing transaction %d to storage", tx.getId());

            for (var action : current.entrySet()) {
                var dep = dependenciesLocked.get(action.getKey());
                if (dep.data().isPresent() && dep.data.get().version() >= tx.getId()) {
                    Log.trace("Skipping write " + action.getKey() + " - dependency " + dep.data().get().version() + " vs " + tx.getId());
                    continue;
                }

                switch (action.getValue()) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
                        Log.trace("Writing " + action.getKey());
                        var wrapped = new JDataVersionedWrapper<>(write.data(), tx.getId());
                        _objects.put(action.getKey(), new JDataWrapper<>(wrapped));
                    }
                    case TxRecord.TxObjectRecordDeleted deleted -> {
                        Log.trace("Deleting " + action.getKey());
                        _objects.remove(action.getKey());
                    }
                    default -> {
                        throw new TxCommitException("Unexpected value: " + action.getValue());
                    }
                }
            }

            Log.tracef("Committing transaction %d to storage", tx.getId());
            var addFlushCallback = writebackObjectPersistentStore.commitTx(current.values(), tx.getId());

            for (var callback : tx.getOnCommit()) {
                callback.run();
            }

            for (var callback : tx.getOnFlush()) {
                addFlushCallback.accept(callback);
            }

            return new TransactionHandle() {
                @Override
                public long getId() {
                    return tx.getId();
                }

                @Override
                public void onFlush(Runnable runnable) {
                    addFlushCallback.accept(runnable);
                }
            };
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
        verifyReady();
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
            var got = getObj(type, key);
            if (got.data().isPresent() && got.data().get().version() > _txId) {
                throw new TxCommitException("Serialization race for " + key + ": " + got.data().get().version() + " vs " + _txId);
            }
            return got;
        }

        @Override
        public <T extends JData> TransactionObject<T> getWriteLocked(Class<T> type, JObjectKey key) {
            var got = getObjLock(type, key);
            if (got.data().isPresent() && got.data().get().version() > _txId) {
                got.lock().close();
                throw new TxCommitException("Serialization race for " + key + ": " + got.data().get().version() + " vs " + _txId);
            }
            return got;
        }
    }
}