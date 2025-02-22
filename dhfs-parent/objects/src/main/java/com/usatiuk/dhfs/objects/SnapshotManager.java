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
import java.util.function.Consumer;

@ApplicationScoped
public class SnapshotManager {
    @Inject
    WritebackObjectPersistentStore delegateStore;

    private interface SnapshotEntry {
        long whenToRemove();

        SnapshotEntry withWhenToRemove(long whenToRemove);
    }

    private record SnapshotEntryRead(JDataVersionedWrapper data, long whenToRemove) implements SnapshotEntry {
        @Override
        public SnapshotEntry withWhenToRemove(long whenToRemove) {
            return new SnapshotEntryRead(data, whenToRemove);
        }
    }

    private record SnapshotEntryReadEmpty(long whenToRemove) implements SnapshotEntry {
        @Override
        public SnapshotEntry withWhenToRemove(long whenToRemove) {
            return new SnapshotEntryReadEmpty(whenToRemove);
        }
    }

    private record SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) implements SnapshotEntry {
        @Override
        public SnapshotEntry withWhenToRemove(long whenToRemove) {
            return new SnapshotEntryObject(data, whenToRemove);
        }
    }

    private record SnapshotEntryDeleted(long whenToRemove) implements SnapshotEntry {
        @Override
        public SnapshotEntry withWhenToRemove(long whenToRemove) {
            return new SnapshotEntryDeleted(whenToRemove);
        }
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

    private final Queue<Long> _snapshotIds = new ArrayDeque<>();
    private final ConcurrentSkipListMap<SnapshotKey, SnapshotEntry> _objects = new ConcurrentSkipListMap<>();
    private final TreeMap<Long, ArrayDeque<SnapshotKey>> _snapshotBounds = new TreeMap<>();
    private final HashMap<Long, Long> _snapshotRefCounts = new HashMap<>();
    private final ConcurrentSkipListMap<Long, Long> _snapshotVersions = new ConcurrentSkipListMap<>();

    private void verify() {
        assert _snapshotIds.isEmpty() == (_lastAliveSnapshotId == -1);
        assert _snapshotIds.isEmpty() || _snapshotIds.peek() == _lastAliveSnapshotId;
    }

    Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes, long id) {
        synchronized (this) {
            assert id > _lastSnapshotId;
            if (!_snapshotIds.isEmpty()) {
                verify();
                boolean hadBackward = false;
                for (var action : writes) {
                    var current = delegateStore.readObjectVerbose(action.key());
                    Pair<SnapshotKey, SnapshotEntry> newSnapshotEntry = switch (current) {
                        case WritebackObjectPersistentStore.VerboseReadResultPersisted(
                                Optional<JDataVersionedWrapper> data
                        ) -> {
                            hadBackward = true;
                            yield Pair.of(new SnapshotKey(action.key(), _snapshotIds.peek()),
                                    data.<SnapshotEntry>map(o -> new SnapshotEntryRead(o, id)).orElse(new SnapshotEntryReadEmpty(id)));
                        }
                        case WritebackObjectPersistentStore.VerboseReadResultPending(
                                TxWriteback.PendingWriteEntry pending
                        ) -> {
                            assert pending.bundleId() < id;
                            yield switch (pending) {
                                case TxWriteback.PendingWrite write ->
                                        Pair.of(new SnapshotKey(action.key(), write.bundleId()), new SnapshotEntryObject(write.data(), write.bundleId()));
                                case TxWriteback.PendingDelete delete ->
                                        Pair.of(new SnapshotKey(action.key(), delete.bundleId()), new SnapshotEntryDeleted(delete.bundleId()));
                                default -> throw new IllegalStateException("Unexpected value: " + pending);
                            };
                        }
                        default -> throw new IllegalStateException("Unexpected value: " + current);
                    };

                    Log.tracev("Adding snapshot entry {0}", newSnapshotEntry);

                    var val = _objects.put(newSnapshotEntry.getLeft(), newSnapshotEntry.getRight());
//                    assert val == null;
                    _snapshotBounds.merge(newSnapshotEntry.getLeft().version(), new ArrayDeque<>(List.of(newSnapshotEntry.getLeft())),
                            (a, b) -> {
                                a.addAll(b);
                                return a;
                            });
                }

//                if (hadBackward)
                for (var sid : _snapshotIds) {
                    _snapshotVersions.merge(sid, 1L, Long::sum);
                }
            }

            verify();
            return delegateStore.commitTx(writes, id);
        }
    }

    private void unrefSnapshot(long id) {
        Log.tracev("Unref snapshot {0}", id);
        synchronized (this) {
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
                _snapshotVersions.remove(curId);
                nextId = _snapshotIds.isEmpty() ? -1 : _snapshotIds.peek();

                var keys = _snapshotBounds.headMap(curId, true);

                long finalCurId = curId;
                long finalNextId = nextId;
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
                        if (!(entry instanceof SnapshotEntryRead || entry instanceof SnapshotEntryReadEmpty)) {
                            Log.errorv("Unexpected entry type: {0}, key: {1}, nextId: {2}, whenToRemove: {3}, snapshotIds: {4}",
                                    entry, key, finalNextId, entry.whenToRemove(), _snapshotIds);
                            assert false;
                        }
                        _objects.put(new SnapshotKey(key.key(), finalNextId), entry);
                        _snapshotBounds.merge(finalNextId, new ArrayDeque<>(List.of(new SnapshotKey(key.key(), finalNextId))),
                                (a, b) -> {
                                    a.addAll(b);
                                    return a;
                                });
                    }
                    _objects.remove(key);
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
            synchronized (SnapshotManager.this) {
                verify();
                if (_lastSnapshotId > id)
                    throw new IllegalSnapshotIdException("Snapshot id " + id + " is less than last snapshot id " + _lastSnapshotId);
                _lastSnapshotId = id;
                if (_lastAliveSnapshotId == -1)
                    _lastAliveSnapshotId = id;
                if (_snapshotRefCounts.merge(id, 1L, Long::sum) == 1) {
                    _snapshotIds.add(id);
                    _snapshotVersions.put(id, 0L);
                }
                verify();
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
                    if (next.getKey().version() <= _id && next.getValue().whenToRemove() > _id) {
                        _next = switch (next.getValue()) {
                            case SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Data<>(data));
                            case SnapshotEntryRead(JDataVersionedWrapper data, long whenToRemove) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Data<>(data));
                            case SnapshotEntryReadEmpty(long whenToRemove) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Tombstone<>());
                            case SnapshotEntryDeleted(long whenToRemove) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Tombstone<>());
                            default -> throw new IllegalStateException("Unexpected value: " + next.getValue());
                        };
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
                _next = null;
                fillNext();
                Log.tracev("Read: {0}, next: {1}", ret, _next);
                return ret;
            }

        }

        public class AutoRefreshingSnapshotKvIterator implements CloseableKvIterator<JObjectKey, JDataVersionedWrapper> {
            private CloseableKvIterator<JObjectKey, JDataVersionedWrapper> _backing;
            private long _lastRefreshed = -1L;
            private Pair<JObjectKey, JDataVersionedWrapper> _next;

            public AutoRefreshingSnapshotKvIterator(IteratorStart start, JObjectKey key) {
                synchronized (SnapshotManager.this) {
                    long curVersion = _snapshotVersions.get(_id);
                    _backing = new TombstoneMergingKvIterator<>(new SnapshotKvIterator(start, key), delegateStore.getIterator(start, key));
                    _next = _backing.hasNext() ? _backing.next() : null;
                    _lastRefreshed = curVersion;
                }
            }

            private void doRefresh() {
                long curVersion = _snapshotVersions.get(_id);
                if (curVersion == _lastRefreshed) {
                    return;
                }
                if (_next == null) return;
                synchronized (SnapshotManager.this) {
                    curVersion = _snapshotVersions.get(_id);
                    Log.tracev("Refreshing snapshot iterator {0}, last refreshed {1}, current version {2}", _id, _lastRefreshed, curVersion);
                    _backing.close();
                    _backing = new TombstoneMergingKvIterator<>(new SnapshotKvIterator(IteratorStart.GE, _next.getKey()), delegateStore.getIterator(IteratorStart.GE, _next.getKey()));
                    var next = _backing.hasNext() ? _backing.next() : null;
                    assert next != null;
                    assert next.equals(_next);
                    _next = next;
                    _lastRefreshed = curVersion;
                }
            }

            private void prepareNext() {
                doRefresh();
                if (_backing.hasNext()) {
                    _next = _backing.next();
                } else {
                    _next = null;
                }
            }

            @Override
            public JObjectKey peekNextKey() {
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
            public Pair<JObjectKey, JDataVersionedWrapper> next() {
                if (_next == null) {
                    throw new NoSuchElementException("No more elements");
                }
                var ret = _next;
                prepareNext();
                Log.tracev("Read: {0}, next: {1}", ret, _next);
                return ret;
            }
        }

        public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
            return new AutoRefreshingSnapshotKvIterator(start, key);
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
