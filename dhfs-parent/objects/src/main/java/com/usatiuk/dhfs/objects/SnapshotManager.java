package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@ApplicationScoped
public class SnapshotManager {
    @Inject
    WritebackObjectPersistentStore delegateStore;

    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    private interface SnapshotEntry {
        long whenToRemove();
    }

    private record SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) implements SnapshotEntry {
    }

    private record SnapshotEntryDeleted(long whenToRemove) implements SnapshotEntry {
    }

    private record SnapshotKey(JObjectKey key, long version) implements Comparable<SnapshotKey> {
        @Override
        public int compareTo(@Nonnull SnapshotKey o) {
            return Comparator.comparing(SnapshotKey::key)
                    .thenComparing(SnapshotKey::version)
                    .compare(this, o);
        }
    }

    private long _lastSnapshotId = 0;
    private long _lastAliveSnapshotId = -1;
    private final AtomicLong _snapshotVersion = new AtomicLong(0);

    private final Queue<Long> _snapshotIds = new ArrayDeque<>();
    private final ConcurrentSkipListMap<SnapshotKey, SnapshotEntry> _objects = new ConcurrentSkipListMap<>();
    private final TreeMap<Long, ArrayDeque<SnapshotKey>> _snapshotBounds = new TreeMap<>();
    private final HashMap<Long, Long> _snapshotRefCounts = new HashMap<>();

    private void verify() {
        assert _snapshotIds.isEmpty() == (_lastAliveSnapshotId == -1);
        assert _snapshotIds.isEmpty() || _snapshotIds.peek() == _lastAliveSnapshotId;
    }

    Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes, long id) {
        _lock.writeLock().lock();
        try {
            assert id > _lastSnapshotId;
            if (!_snapshotIds.isEmpty()) {
                verify();
                for (var action : writes) {
                    var current = delegateStore.readObjectVerbose(action.key());
                    // Add to snapshot the previous visible version of the replaced object
                    // I.e. should be visible to all transactions with id <= id
                    // and at least as its corresponding version
                    Pair<SnapshotKey, SnapshotEntry> newSnapshotEntry = switch (current) {
                        case WritebackObjectPersistentStore.VerboseReadResultPersisted(
                                Optional<JDataVersionedWrapper> data
                        ) ->
                                Pair.of(new SnapshotKey(action.key(), Math.max(_snapshotIds.peek(), data.map(JDataVersionedWrapper::version).orElse(0L))),
                                        data.<SnapshotEntry>map(o -> new SnapshotEntryObject(o, id)).orElse(new SnapshotEntryDeleted(id)));
                        case WritebackObjectPersistentStore.VerboseReadResultPending(
                                TxWriteback.PendingWriteEntry pending
                        ) -> {
                            assert pending.bundleId() < id;
                            yield switch (pending) {
                                case TxWriteback.PendingWrite write ->
                                        Pair.of(new SnapshotKey(action.key(), write.bundleId()), new SnapshotEntryObject(write.data(), id));
                                case TxWriteback.PendingDelete delete ->
                                        Pair.of(new SnapshotKey(action.key(), delete.bundleId()), new SnapshotEntryDeleted(id));
                                default -> throw new IllegalStateException("Unexpected value: " + pending);
                            };
                        }
                        default -> throw new IllegalStateException("Unexpected value: " + current);
                    };

                    if (newSnapshotEntry.getValue() instanceof SnapshotEntryObject re) {
                        assert re.data().version() <= newSnapshotEntry.getKey().version();
                    }
                    if (newSnapshotEntry.getValue() instanceof SnapshotEntryObject re) {
                        assert re.data().version() <= newSnapshotEntry.getKey().version();
                    }

                    Log.tracev("Adding snapshot entry {0}", newSnapshotEntry);

                    var val = _objects.put(newSnapshotEntry.getLeft(), newSnapshotEntry.getRight());
//                    assert val == null;
                    _snapshotBounds.merge(newSnapshotEntry.getLeft().version(), new ArrayDeque<>(List.of(newSnapshotEntry.getLeft())),
                            (a, b) -> {
                                a.addAll(b);
                                return a;
                            });
                }

                _snapshotVersion.incrementAndGet();
            }

            verify();
            // Commit under lock, iterators will see new version after the lock is released and writeback
            // cache is updated
            // TODO: Maybe writeback iterator being invalidated wouldn't be a problem?
            return delegateStore.commitTx(writes, id);
        } finally {
            _lock.writeLock().unlock();
        }
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
                        _objects.put(new SnapshotKey(key.key(), finalNextId), entry);
                        assert finalNextId > finalCurId;
                        toReAdd.add(Pair.of(finalNextId, new SnapshotKey(key.key(), finalNextId)));
                    }
                    _objects.remove(key);
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

        public class SnapshotKvIterator implements CloseableKvIterator<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> {
            private final CloseableKvIterator<SnapshotKey, SnapshotEntry> _backing;
            private Pair<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> _next;

            public SnapshotKvIterator(IteratorStart start, JObjectKey key) {
                _backing = new NavigableMapKvIterator<>(_objects, start, new SnapshotKey(key, 0L));
                fillNext();
            }

            private void fillNext() {
                while (_backing.hasNext() && _next == null) {
                    var next = _backing.next();
                    var nextNextKey = _backing.hasNext() ? _backing.peekNextKey() : null;
                    while (nextNextKey != null && nextNextKey.key.equals(next.getKey().key()) && nextNextKey.version() <= _id) {
                        next = _backing.next();
                        nextNextKey = _backing.hasNext() ? _backing.peekNextKey() : null;
                    }
                    // next.getValue().whenToRemove() >=_id, read tx might have same snapshot id as some write tx
                    if (next.getKey().version() <= _id && next.getValue().whenToRemove() > _id) {
                        _next = switch (next.getValue()) {
                            case SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Data<>(data));
                            case SnapshotEntryDeleted(long whenToRemove) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Tombstone<>());
                            default -> throw new IllegalStateException("Unexpected value: " + next.getValue());
                        };
                    }
                    if (_next != null) {
                        if (_next.getValue() instanceof TombstoneMergingKvIterator.Data<JDataVersionedWrapper>(
                                JDataVersionedWrapper value
                        )) {
                            assert value.version() <= _id;
                        }
                    }
                }
            }

            @Override
            public JObjectKey peekNextKey() {
                if (_next == null)
                    throw new NoSuchElementException();
                return _next.getKey();
            }

            @Override
            public void close() {
                _backing.close();
            }

            @Override
            public boolean hasNext() {
                return _next != null;
            }

            @Override
            public Pair<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> next() {
                if (_next == null)
                    throw new NoSuchElementException("No more elements");
                var ret = _next;
                if (ret.getValue() instanceof TombstoneMergingKvIterator.Data<JDataVersionedWrapper>(
                        JDataVersionedWrapper value
                )) {
                    assert value.version() <= _id;
                }

                _next = null;
                fillNext();
                Log.tracev("Read: {0}, next: {1}", ret, _next);
                return ret;
            }

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
            // In case something was added to the snapshot, it is not guaranteed that the iterators will see it,
            // so refresh them manually. Otherwise, it could be possible that something from the writeback cache will
            // be served instead. Note that refreshing the iterator will also refresh the writeback iterator,
            // so it also should be consistent.
            return new CheckingSnapshotKvIterator(new SelfRefreshingKvIterator<>((params) ->
                    new TombstoneMergingKvIterator<>(new SnapshotKvIterator(params.getLeft(), params.getRight()),
                            new MappingKvIterator<>(delegateStore.getIterator(params.getLeft(), params.getRight()), d -> switch (d) {
                                case TombstoneMergingKvIterator.Tombstone<JDataVersionedWrapper>() -> d;
                                case TombstoneMergingKvIterator.Data<JDataVersionedWrapper> data ->
                                        data.value().version() <= _id ? data : new TombstoneMergingKvIterator.Tombstone<>();
                                default -> throw new IllegalStateException("Unexpected value: " + d);
                            })), _snapshotVersion::get, _lock.readLock(), start, key));
        }

        public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(JObjectKey key) {
            return getIterator(IteratorStart.GE, key);
        }

        @Nonnull
        public Optional<JDataVersionedWrapper> readObject(JObjectKey name) {
            try (var it = getIterator(name)) {
                if (it.hasNext()) {
                    var read = it.next();
                    if (read.getKey().equals(name)) {
                        return Optional.of(read.getValue());
                    }
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

    public Snapshot createSnapshot(long id) {
        return new Snapshot(id);
    }

    @Nonnull
    Optional<JDataVersionedWrapper> readObjectDirect(JObjectKey name) {
        return delegateStore.readObject(name);
    }
}
