package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pcollections.TreePMap;

import javax.annotation.Nonnull;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@ApplicationScoped
public class SnapshotManager {
    @Inject
    WritebackObjectPersistentStore writebackStore;

    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    @ConfigProperty(name = "dhfs.objects.persistence.snapshot-extra-checks")
    boolean extraChecks;

    private long _lastSnapshotId = 0;
    private long _lastAliveSnapshotId = -1;

    private final Queue<Long> _snapshotIds = new ArrayDeque<>();
    private TreePMap<SnapshotKey, SnapshotEntry> _objects = TreePMap.empty();
    private final TreeMap<Long, ArrayDeque<SnapshotKey>> _snapshotBounds = new TreeMap<>();
    private final HashMap<Long, Long> _snapshotRefCounts = new HashMap<>();

    private void verify() {
        assert _snapshotIds.isEmpty() == (_lastAliveSnapshotId == -1);
        assert _snapshotIds.isEmpty() || _snapshotIds.peek() == _lastAliveSnapshotId;
    }

    // This should not be called for the same objects concurrently
    public Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes) {
//        _lock.writeLock().lock();
//        try {
//        if (!_snapshotIds.isEmpty()) {
//        verify();
        HashMap<SnapshotKey, SnapshotEntry> newEntries = new HashMap<>();
        for (var action : writes) {
            var current = writebackStore.readObjectVerbose(action.key());
            // Add to snapshot the previous visible version of the replaced object
            // I.e. should be visible to all transactions with id <= id
            // and at least as its corresponding version
            Pair<SnapshotKey, SnapshotEntry> newSnapshotEntry = switch (current) {
                case WritebackObjectPersistentStore.VerboseReadResultPersisted(
                        Optional<JDataVersionedWrapper> data
                ) -> Pair.of(new SnapshotKey(action.key(), data.map(JDataVersionedWrapper::version).orElse(-1L)),
                        data.<SnapshotEntry>map(o -> new SnapshotEntryObject(o, -1)).orElse(new SnapshotEntryDeleted(-1)));
                case WritebackObjectPersistentStore.VerboseReadResultPending(
                        PendingWriteEntry pending
                ) -> {
                    yield switch (pending) {
                        case PendingWrite write ->
                                Pair.of(new SnapshotKey(action.key(), write.bundleId()), new SnapshotEntryObject(write.data(), -1));
                        case PendingDelete delete ->
                                Pair.of(new SnapshotKey(action.key(), delete.bundleId()), new SnapshotEntryDeleted(-1));
                        default -> throw new IllegalStateException("Unexpected value: " + pending);
                    };
                }
                default -> throw new IllegalStateException("Unexpected value: " + current);
            };


            Log.tracev("Adding snapshot entry {0}", newSnapshotEntry);

            newEntries.put(newSnapshotEntry.getLeft(), newSnapshotEntry.getRight());
        }

        _lock.writeLock().lock();
        try {
            return writebackStore.commitTx(writes, (id, commit) -> {
                if (!_snapshotIds.isEmpty()) {
                    assert id > _lastSnapshotId;
                    for (var newSnapshotEntry : newEntries.entrySet()) {
                        assert newSnapshotEntry.getKey().version() < id;
                        var realNewSnapshotEntry = newSnapshotEntry.getValue().withWhenToRemove(id);
                        if (realNewSnapshotEntry instanceof SnapshotEntryObject re) {
                            assert re.data().version() <= newSnapshotEntry.getKey().version();
                        }
                        _objects = _objects.plus(newSnapshotEntry.getKey(), realNewSnapshotEntry);
//                    assert val == null;
                        _snapshotBounds.merge(newSnapshotEntry.getKey().version(), new ArrayDeque<>(List.of(newSnapshotEntry.getKey())),
                                (a, b) -> {
                                    a.addAll(b);
                                    return a;
                                });
                    }
                }
                commit.run();
            });
        } finally {
            _lock.writeLock().unlock();
        }

//        }

//        verify();
        // Commit under lock, iterators will see new version after the lock is released and writeback
        // cache is updated
        // TODO: Maybe writeback iterator being invalidated wouldn't be a problem?
//        } finally {
//            _lock.writeLock().unlock();
//        }
    }

    private void unrefSnapshot(long id) {
        Log.tracev("Unref snapshot {0}", id);
        _lock.writeLock().lock();
        try {
            verify();
            var refCount = _snapshotRefCounts.merge(id, -1L, (a, b) -> a + b == 0 ? null : a + b);
            if (!(refCount == null && id == _lastAliveSnapshotId)) {
                return;
            }

            long curCount;
            long curId = id;
            long nextId;
            do {
                Log.tracev("Removing snapshot {0}", curId);
                _snapshotIds.poll();
                nextId = _snapshotIds.isEmpty() ? -1 : _snapshotIds.peek();
                while (nextId == curId) {
                    _snapshotIds.poll();
                    nextId = _snapshotIds.isEmpty() ? -1 : _snapshotIds.peek();
                }

                var keys = _snapshotBounds.headMap(curId, true);

                long finalCurId = curId;
                long finalNextId = nextId;
                ArrayList<Pair<Long, SnapshotKey>> toReAdd = new ArrayList<>();
                keys.values().stream().flatMap(Collection::stream).forEach(key -> {
                    var entry = _objects.get(key);
                    if (entry == null) {
//                        Log.warnv("Entry not found for key {0}", key);
                        return;
                    }
                    if (finalNextId == -1) {
                        Log.tracev("Could not find place to place entry {0}, curId={1}, nextId={2}, whenToRemove={3}, snapshotIds={4}",
                                entry, finalCurId, finalNextId, entry.whenToRemove(), _snapshotIds);
                    } else if (finalNextId < entry.whenToRemove()) {
                        _objects = _objects.plus(new SnapshotKey(key.key(), finalNextId), entry);
                        assert finalNextId > finalCurId;
                        toReAdd.add(Pair.of(finalNextId, new SnapshotKey(key.key(), finalNextId)));
                    }
                    _objects = _objects.minus(key);
                });

                toReAdd.forEach(p -> {
                    _snapshotBounds.merge(p.getLeft(), new ArrayDeque<>(List.of(p.getRight())),
                            (a, b) -> {
                                a.addAll(b);
                                return a;
                            });
                });

                keys.clear();

                if (_snapshotIds.isEmpty()) {
                    _lastAliveSnapshotId = -1;
                    break;
                }

                curId = _snapshotIds.peek();
                _lastAliveSnapshotId = curId;

                curCount = _snapshotRefCounts.getOrDefault(curId, 0L);
            } while (curCount == 0);
            verify();
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public static class IllegalSnapshotIdException extends IllegalArgumentException {
        public IllegalSnapshotIdException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    public class Snapshot implements AutoCloseableNoThrow {
        private final long _id;
        private static final Cleaner CLEANER = Cleaner.create();
        private final MutableObject<Boolean> _closed = new MutableObject<>(false);

        public long id() {
            return _id;
        }

        private Snapshot(long id) {
            _id = id;
            _lock.writeLock().lock();
            try {
                verify();
                if (_lastSnapshotId > id)
                    throw new IllegalSnapshotIdException("Snapshot id " + id + " is less than last snapshot id " + _lastSnapshotId);
                _lastSnapshotId = id;
                if (_lastAliveSnapshotId == -1)
                    _lastAliveSnapshotId = id;
                if (_snapshotRefCounts.merge(id, 1L, Long::sum) == 1) {
                    _snapshotIds.add(id);
                }
                verify();
            } finally {
                _lock.writeLock().unlock();
            }
            var closedRef = _closed;
            var idRef = _id;
            CLEANER.register(this, () -> {
                if (!closedRef.getValue()) {
                    Log.error("Snapshot " + idRef + " was not closed before GC");
                }
            });
        }

        public class CheckingSnapshotKvIterator implements CloseableKvIterator<JObjectKey, JDataVersionedWrapper> {
            private final CloseableKvIterator<JObjectKey, JDataVersionedWrapper> _backing;

            public CheckingSnapshotKvIterator(CloseableKvIterator<JObjectKey, JDataVersionedWrapper> backing) {
                _backing = backing;
            }

            @Override
            public JObjectKey peekNextKey() {
                return _backing.peekNextKey();
            }

            @Override
            public void skip() {
                _backing.skip();
            }

            @Override
            public JObjectKey peekPrevKey() {
                return _backing.peekPrevKey();
            }

            @Override
            public Pair<JObjectKey, JDataVersionedWrapper> prev() {
                var ret = _backing.prev();
                assert ret.getValue().version() <= _id;
                return ret;
            }

            @Override
            public boolean hasPrev() {
                return _backing.hasPrev();
            }

            @Override
            public void skipPrev() {
                _backing.skipPrev();
            }

            @Override
            public void close() {
                _backing.close();
            }

            @Override
            public boolean hasNext() {
                return _backing.hasNext();
            }

            @Override
            public Pair<JObjectKey, JDataVersionedWrapper> next() {
                var ret = _backing.next();
                assert ret.getValue().version() <= _id;
                return ret;
            }
        }

        public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
            _lock.readLock().lock();
            try {
                Log.tracev("Getting snapshot {0} iterator for {1} {2}\n" +
                        "objects in snapshots: {3}", _id, start, key, _objects);
                return new CheckingSnapshotKvIterator(new TombstoneMergingKvIterator<>("snapshot", start, key,
                        (tS, tK) -> new SnapshotKvIterator(_objects, _id, tS, tK),
                        (tS, tK) -> new MappingKvIterator<>(
                                writebackStore.getIterator(tS, tK), d -> d.version() <= _id ? new Data<>(d) : new Tombstone<>())
                ));
            } finally {
                _lock.readLock().unlock();
            }
        }

        @Nonnull
        public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
            try (var it = getIterator(IteratorStart.GE, name)) {
                if (it.hasNext()) {
                    if (!it.peekNextKey().equals(name)) {
                        return Optional.empty();
                    }
                    return Optional.of(it.next().getValue());
                }
            }
            return Optional.empty();
        }

        @Override
        public void close() {
            if (_closed.getValue()) {
                return;
            }
            _closed.setValue(true);
            unrefSnapshot(_id);
        }
    }

    public Snapshot createSnapshot() {
        _lock.writeLock().lock();
        try {
            return new Snapshot(writebackStore.getLastTxId());
        } finally {
            _lock.writeLock().unlock();
        }
    }

    @Nonnull
    public Optional<JDataVersionedWrapper> readObjectDirect(JObjectKey name) {
        return writebackStore.readObject(name);
    }
}
