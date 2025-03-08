package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.snapshot.SnapshotManager;
import com.usatiuk.dhfs.objects.transaction.*;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.*;
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
    private boolean _ready = false;
    @Inject
    SnapshotManager snapshotManager;
    @Inject
    TransactionFactory transactionFactory;
    @Inject
    LockManager lockManager;

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    void init(@Observes @Priority(200) StartupEvent event) {
        _ready = true;
    }

    JObjectManager(Instance<PreCommitTxHook> preCommitTxHooks) {
        _preCommitTxHooks = preCommitTxHooks.stream().sorted(Comparator.comparingInt(PreCommitTxHook::getPriority)).toList();
    }

    public TransactionPrivate createTransaction() {
        verifyReady();
        var tx = transactionFactory.createTransaction();
        Log.tracev("Created transaction with snapshotId={0}", tx.snapshot().id());
        return tx;
    }

    public TransactionHandle commit(TransactionPrivate tx) {
        verifyReady();
        var writes = new LinkedHashMap<JObjectKey, TxRecord.TxObjectRecord<?>>();
        var dependenciesLocked = new LinkedHashMap<JObjectKey, Optional<JDataVersionedWrapper>>();
        Map<JObjectKey, TransactionObject<?>> readSet;
        var toUnlock = new ArrayList<AutoCloseableNoThrow>();

        Consumer<JObjectKey> addDependency =
                key -> {
                    dependenciesLocked.computeIfAbsent(key, k -> {
                        var lock = lockManager.lockObject(k);
                        toUnlock.add(lock);
                        return snapshotManager.readObjectDirect(k);
                    });
                };

        // For existing objects:
        // Check that their version is not higher than the version of transaction being committed
        // TODO: check deletions, inserts
        try {
            try {
                Function<JObjectKey, JData> getCurrent =
                        key -> switch (writes.get(key)) {
                            case TxRecord.TxObjectRecordWrite<?> write -> write.data();
                            case TxRecord.TxObjectRecordDeleted deleted -> null;
                            case null -> tx.readSource().get(JData.class, key).orElse(null);
                            default -> {
                                throw new TxCommitException("Unexpected value: " + writes.get(key));
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
                    writes.putAll(currentIteration);
                } while (somethingChanged);

                if (writes.isEmpty()) {
                    Log.trace("Committing transaction - no changes");
                    return new TransactionHandle() {
                        @Override
                        public void onFlush(Runnable runnable) {
                            runnable.run();
                        }
                    };
                }

            } finally {
                readSet = tx.reads();

                Stream.concat(readSet.keySet().stream(), writes.keySet().stream())
                        .sorted(Comparator.comparing(JObjectKey::toString))
                        .forEach(addDependency);

                for (var read : readSet.entrySet()) {
                    if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                        toUnlock.add(locked.lock());
                    }
                }
            }

            Log.trace("Committing transaction start");
            var snapshotId = tx.snapshot().id();

            for (var read : readSet.entrySet()) {
                var dep = dependenciesLocked.get(read.getKey());

                if (dep.isEmpty() != read.getValue().data().isEmpty()) {
                    Log.trace("Checking read dependency " + read.getKey() + " - not found");
                    throw new TxCommitException("Serialization hazard: " + dep.isEmpty() + " vs " + read.getValue().data().isEmpty());
                }

                if (dep.isEmpty()) {
                    // TODO: Every write gets a dependency due to hooks
                    continue;
//                    assert false;
//                    throw new TxCommitException("Serialization hazard: " + dep.isEmpty() + " vs " + read.getValue().data().isEmpty());
                }

                if (dep.get().version() > snapshotId) {
                    Log.trace("Checking dependency " + read.getKey() + " - newer than");
                    throw new TxCommitException("Serialization hazard: " + dep.get().data().key() + " " + dep.get().version() + " vs " + snapshotId);
                }

                Log.trace("Checking dependency " + read.getKey() + " - ok with read");
            }

            var addFlushCallback = snapshotManager.commitTx(
                    writes.values().stream()
                            .filter(r -> {
                                if (r instanceof TxRecord.TxObjectRecordWrite<?>(JData data)) {
                                    var dep = dependenciesLocked.get(data.key());
                                    if (dep.isPresent() && dep.get().version() > snapshotId) {
                                        Log.trace("Skipping write " + data.key() + " - dependency " + dep.get().version() + " vs " + snapshotId);
                                        return false;
                                    }
                                }
                                return true;
                            }).toList());

            for (var callback : tx.getOnCommit()) {
                callback.run();
            }

            for (var callback : tx.getOnFlush()) {
                addFlushCallback.accept(callback);
            }

            return new TransactionHandle() {
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
            tx.close();
        }
    }

    public void rollback(TransactionPrivate tx) {
        verifyReady();
        tx.reads().forEach((key, value) -> {
            if (value instanceof TransactionObjectLocked<?> locked) {
                locked.lock().close();
            }
        });
        tx.close();
    }

    //    private class TransactionObjectSourceImpl implements TransactionObjectSource {
//        private final long _txId;
//
//        private TransactionObjectSourceImpl(long txId) {
//            _txId = txId;
//        }
//
//        @Override
//        public <T extends JData> TransactionObject<T> get(Class<T> type, JObjectKey key) {
//            var got = getObj(type, key);
//            if (got.data().isPresent() && got.data().get().version() > _txId) {
//                throw new TxCommitException("Serialization race for " + key + ": " + got.data().get().version() + " vs " + _txId);
//            }
//            return got;
//        }
//
//        @Override
//        public <T extends JData> TransactionObject<T> getWriteLocked(Class<T> type, JObjectKey key) {
//            var got = getObjLock(type, key);
//            if (got.data().isPresent() && got.data().get().version() > _txId) {
//                got.lock().close();
//                throw new TxCommitException("Serialization race for " + key + ": " + got.data().get().version() + " vs " + _txId);
//            }
//            return got;
//        }
//    }
}