package com.usatiuk.objects.transaction;

import com.google.common.collect.Streams;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.objects.snapshot.SnapshotManager;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

// Manages all access to com.usatiuk.objects.JData objects.
// In particular, it serves as a source of truth for what is committed to the backing storage.
// All data goes through it, it is responsible for transaction atomicity
// TODO: persistent tx id
@ApplicationScoped
public class JObjectManager {
    private final List<PreCommitTxHook> _preCommitTxHooks;

    private record CommitHookIterationData(PreCommitTxHook hook,
                                           Map<JObjectKey, TxRecord.TxObjectRecord<?>> lastWrites,
                                           Map<JObjectKey, TxRecord.TxObjectRecord<?>> pendingWrites) {
    }

    @Inject
    SnapshotManager snapshotManager;
    @Inject
    TransactionFactory transactionFactory;
    @Inject
    LockManager lockManager;
    private boolean _ready = false;

    JObjectManager(Instance<PreCommitTxHook> preCommitTxHooks) {
        _preCommitTxHooks = List.copyOf(preCommitTxHooks.stream().sorted(Comparator.comparingInt(PreCommitTxHook::getPriority)).toList());
        Log.debugv("Pre-commit hooks: {0}", String.join("->", _preCommitTxHooks.stream().map(Objects::toString).toList()));
    }

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    void init(@Observes @Priority(200) StartupEvent event) {
        _ready = true;
    }

    public TransactionPrivate createTransaction() {
        verifyReady();
        var tx = transactionFactory.createTransaction();
        Log.tracev("Created transaction with snapshotId={0}", tx.snapshot().id());
        return tx;
    }

    public Pair<Collection<Runnable>, TransactionHandle> commit(TransactionPrivate tx) {
        verifyReady();
        var writes = new HashMap<JObjectKey, TxRecord.TxObjectRecord<?>>();
        Snapshot<JObjectKey, JDataVersionedWrapper> commitSnapshot = null;
        Map<JObjectKey, TransactionObject<?>> readSet = null;
        Collection<AutoCloseableNoThrow> toUnlock = null;

        try {
            try {
                long pendingCount = 0;
                List<CommitHookIterationData> hookIterationData;
                {
                    CommitHookIterationData[] hookIterationDataArray = new CommitHookIterationData[_preCommitTxHooks.size()];
                    for (int i = 0; i < _preCommitTxHooks.size(); i++) {
                        var hook = _preCommitTxHooks.get(i);
                        hookIterationDataArray[i] = new CommitHookIterationData(hook, new HashMap<>(), new HashMap<>());
                    }
                    hookIterationData = List.of(hookIterationDataArray);
                }

                for (var n : tx.drainNewWrites()) {
                    for (var hookPut : hookIterationData) {
                        hookPut.pendingWrites().put(n.key(), n);
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
                while (pendingCount > 0) {
                    for (var hookId : hookIterationData) {
                        var hook = hookId.hook();
                        var lastCurHookSeen = hookId.lastWrites();
                        Function<JObjectKey, JData> getPrev =
                                key -> switch (lastCurHookSeen.get(key)) {
                                    case TxRecord.TxObjectRecordWrite<?> write -> write.data();
                                    case TxRecord.TxObjectRecordDeleted deleted -> null;
                                    case null -> tx.getFromSource(JData.class, key).orElse(null);
                                    default -> {
                                        throw new TxCommitException("Unexpected value: " + writes.get(key));
                                    }
                                };

                        var curIteration = hookId.pendingWrites();

//                        Log.trace("Commit iteration with " + curIteration.size() + " records for hook " + hook.getClass());

                        for (var entry : curIteration.entrySet()) {
//                            Log.trace("Running pre-commit hook " + hook.getClass() + " for" + entry.getKey());
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
                            for (var hookPut : hookIterationData) {
                                if (hookPut == hookId) {
                                    lastCurHookSeen.put(n.key(), n);
                                    continue;
                                }
                                var before = hookPut.pendingWrites().put(n.key(), n);
                                if (before == null)
                                    pendingCount++;
                            }
                            writes.put(n.key(), n);
                        }
                    }
                }
            } catch (Throwable e) {
                for (var read : tx.reads().entrySet()) {
                    if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                        locked.lock().close();
                    }
                }
                throw e;
            }

            readSet = tx.reads();

            if (!writes.isEmpty()) {
                toUnlock = new ArrayList<>(readSet.size() + writes.size());
                ArrayList<JObjectKey> toLock = new ArrayList<>(readSet.size() + writes.size());
                for (var read : readSet.entrySet()) {
                    if (read.getValue() instanceof TransactionObjectLocked<?> locked) {
                        toUnlock.add(locked.lock());
                    } else {
                        toLock.add(read.getKey());
                    }
                }
                for (var write : writes.entrySet()) {
                    toLock.add(write.getKey());
                }
                Collections.sort(toLock);
                for (var key : toLock) {
                    var lock = lockManager.lockObject(key);
                    toUnlock.add(lock);
                }

                commitSnapshot = snapshotManager.createSnapshot();
            } else {
                Log.trace("Committing transaction - no changes");

                for (var read : readSet.values()) {
                    if (read instanceof TransactionObjectLocked<?> locked) {
                        locked.lock().close();
                    }
                }

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

            if (snapshotId != commitSnapshot.id()) {
                for (var read : readSet.entrySet()) {
                    var current = commitSnapshot.readObject(read.getKey());

                    if (current.isEmpty() != read.getValue().data().isEmpty()) {
                        Log.tracev("Checking read dependency {0} - not found", read.getKey());
                        throw new TxCommitException("Serialization hazard: " + current.isEmpty() + " vs " + read.getValue().data().isEmpty());
                    }

                    if (current.isEmpty()) {
                        // TODO: Every write gets a dependency due to hooks
                        continue;
//                    assert false;
//                    throw new TxCommitException("Serialization hazard: " + dep.isEmpty() + " vs " + read.getValue().data().isEmpty());
                    }

                    if (current.get().version() > snapshotId) {
                        Log.tracev("Checking dependency {0} - newer than", read.getKey());
                        throw new TxCommitException("Serialization hazard: " + current.get().data().key() + " " + current.get().version() + " vs " + snapshotId);
                    }

                    Log.tracev("Checking dependency {0} - ok with read", read.getKey());
                }
            } else {
                Log.tracev("Skipped dependency checks: no changes");
            }

            var addFlushCallback = snapshotManager.commitTx(writes.values());

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
            if (toUnlock != null)
                for (var unlock : toUnlock) {
                    unlock.close();
                }
            if (commitSnapshot != null)
                commitSnapshot.close();
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