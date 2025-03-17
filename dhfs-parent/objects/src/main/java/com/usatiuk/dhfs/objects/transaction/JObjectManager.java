package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.snapshot.SnapshotManager;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

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
        _preCommitTxHooks = List.copyOf(preCommitTxHooks.stream().sorted(Comparator.comparingInt(PreCommitTxHook::getPriority)).toList());
        Log.debugv("Pre-commit hooks: {0}", String.join("->", _preCommitTxHooks.stream().map(Objects::toString).toList()));
    }

    public TransactionPrivate createTransaction() {
        verifyReady();
        var tx = transactionFactory.createTransaction();
        Log.tracev("Created transaction with snapshotId={0}", tx.snapshot().id());
        return tx;
    }

    public Pair<Collection<Runnable>, TransactionHandle> commit(TransactionPrivate tx) {
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

        try {
            try {
                long pendingCount = 0;
                Map<PreCommitTxHook, Map<JObjectKey, TxRecord.TxObjectRecord<?>>> pendingWrites = Map.ofEntries(
                        _preCommitTxHooks.stream().map(p -> Pair.of(p, new HashMap<>())).toArray(Pair[]::new)
                );
                Map<PreCommitTxHook, Map<JObjectKey, TxRecord.TxObjectRecord<?>>> lastWrites = Map.ofEntries(
                        _preCommitTxHooks.stream().map(p -> Pair.of(p, new HashMap<>())).toArray(Pair[]::new)
                );

                for (var n : tx.drainNewWrites()) {
                    for (var hookPut : _preCommitTxHooks) {
                        pendingWrites.get(hookPut).put(n.key(), n);
                        pendingCount++;
                    }
                    writes.put(n.key(), n);
                }

                // Run hooks for all objects
                // Every hook should see every change made to every object, yet the object's evolution
                // should be consistent from the view point of each individual hook
                // For example, when a hook makes changes to an object, and another hook changes the object before/after it
                // on the next iteration, the first hook should receive the version of the object it had created
                // as the "old" version, and the new version with all the changes after it.
                do {
                    for (var hook : _preCommitTxHooks) {
                        var lastCurHookSeen = lastWrites.get(hook);
                        Function<JObjectKey, JData> getPrev =
                                key -> switch (lastCurHookSeen.get(key)) {
                                    case TxRecord.TxObjectRecordWrite<?> write -> write.data();
                                    case TxRecord.TxObjectRecordDeleted deleted -> null;
                                    case null -> tx.getFromSource(JData.class, key).orElse(null);
                                    default -> {
                                        throw new TxCommitException("Unexpected value: " + writes.get(key));
                                    }
                                };

                        var curIteration = pendingWrites.get(hook);

                        Log.trace("Commit iteration with " + curIteration.size() + " records for hook " + hook.getClass());

                        for (var entry : curIteration.entrySet()) {
                            Log.trace("Running pre-commit hook " + hook.getClass() + " for" + entry.getKey());
                            var oldObj = getPrev.apply(entry.getKey());
                            lastCurHookSeen.put(entry.getKey(), entry.getValue());
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

                        pendingCount -= curIteration.size();
                        curIteration.clear();

                        for (var n : tx.drainNewWrites()) {
                            for (var hookPut : _preCommitTxHooks) {
                                if (hookPut == hook) {
                                    lastCurHookSeen.put(n.key(), n);
                                    continue;
                                }
                                var before = pendingWrites.get(hookPut).put(n.key(), n);
                                if (before == null)
                                    pendingCount++;
                            }
                            writes.put(n.key(), n);
                        }
                    }
                } while (pendingCount > 0);
            } catch (Throwable e) {
                for (var read : tx.reads().entrySet()) {
                    if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                        toUnlock.add(locked.lock());
                    }
                }
                throw e;
            }

            readSet = tx.reads();

            if (!writes.isEmpty()) {
                Stream.concat(readSet.keySet().stream(), writes.keySet().stream())
                        .sorted(Comparator.comparing(JObjectKey::toString))
                        .forEach(addDependency);
            }

            for (var read : readSet.entrySet()) {
                if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                    toUnlock.add(locked.lock());
                }
            }

            if (writes.isEmpty()) {
                Log.trace("Committing transaction - no changes");

                return Pair.of(
                        Stream.concat(
                                tx.getOnCommit().stream(),
                                tx.getOnFlush().stream()
                        ).toList(),
                        new TransactionHandle() {
                            @Override
                            public void onFlush(Runnable runnable) {
                                runnable.run();
                            }
                        });
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

            for (var callback : tx.getOnFlush()) {
                addFlushCallback.accept(callback);
            }

            return Pair.of(
                    List.copyOf(tx.getOnCommit()),
                    new TransactionHandle() {
                        @Override
                        public void onFlush(Runnable runnable) {
                            addFlushCallback.accept(runnable);
                        }
                    });
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