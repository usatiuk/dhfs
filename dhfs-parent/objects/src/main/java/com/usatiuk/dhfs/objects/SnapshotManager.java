package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
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
    }

    private record SnapshotEntryObject(JDataVersionedWrapper data) implements SnapshotEntry {
    }

    private record SnapshotEntryDeleted() implements SnapshotEntry {
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
    private final MultiValuedMap<Long, SnapshotKey> _snapshotBounds = new HashSetValuedHashMap<>();
    private final HashMap<Long, Long> _snapshotRefCounts = new HashMap<>();

    private void verify() {
        assert _snapshotIds.isEmpty() == (_lastAliveSnapshotId == -1);
        assert _snapshotIds.isEmpty() || _snapshotIds.peek() == _lastAliveSnapshotId;
    }

    Consumer<Runnable> commitTx(Collection<TxRecord.TxObjectRecord<?>> writes, long id) {
        synchronized (this) {
            if (!_snapshotIds.isEmpty()) {
                verify();
                for (var action : writes) {
                    var current = delegateStore.readObjectVerbose(action.key());
                    Pair<SnapshotKey, SnapshotEntry> newSnapshotEntry = switch (current) {
                        case WritebackObjectPersistentStore.VerboseReadResultPersisted(
                                Optional<JDataVersionedWrapper> data
                        ) -> Pair.of(new SnapshotKey(action.key(), _snapshotIds.peek()),
                                data.<SnapshotEntry>map(SnapshotEntryObject::new).orElse(new SnapshotEntryDeleted()));
                        case WritebackObjectPersistentStore.VerboseReadResultPending(
                                TxWriteback.PendingWriteEntry pending
                        ) -> switch (pending) {
                            case TxWriteback.PendingWrite write ->
                                    Pair.of(new SnapshotKey(action.key(), write.bundleId()), new SnapshotEntryObject(write.data()));
                            case TxWriteback.PendingDelete delete ->
                                    Pair.of(new SnapshotKey(action.key(), delete.bundleId()), new SnapshotEntryDeleted());
                            default -> throw new IllegalStateException("Unexpected value: " + pending);
                        };
                        default -> throw new IllegalStateException("Unexpected value: " + current);
                    };

                    _objects.put(newSnapshotEntry.getLeft(), newSnapshotEntry.getRight());
                    _snapshotBounds.put(newSnapshotEntry.getLeft().version(), newSnapshotEntry.getLeft());
                }
            }

            verify();
            return delegateStore.commitTx(writes, id);
        }
    }

    private void unrefSnapshot(long id) {
        synchronized (this) {
            verify();
            var refCount = _snapshotRefCounts.merge(id, -1L, (a, b) -> a + b == 0 ? null : a + b);
            if (!(refCount == null && id == _lastAliveSnapshotId)) {
                return;
            }

            long curCount;
            long curId = id;
            do {
                _snapshotIds.poll();

                for (var key : _snapshotBounds.remove(curId)) {
                    _objects.remove(key);
                }

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
                    throw new IllegalArgumentException("Snapshot id less than last? " + id + " vs " + _lastSnapshotId);
                _lastSnapshotId = id;
                if (_lastAliveSnapshotId == -1)
                    _lastAliveSnapshotId = id;
                _snapshotIds.add(id);
                _snapshotRefCounts.merge(id, 1L, Long::sum);
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
                        nextNextKey = _backing.peekNextKey();
                    }
                    if (next.getKey().version() <= _id) {
                        _next = switch (next.getValue()) {
                            case SnapshotEntryObject(JDataVersionedWrapper data) ->
                                    Pair.of(next.getKey().key(), new TombstoneMergingKvIterator.Data<>(data));
                            case SnapshotEntryDeleted() ->
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
                return ret;
            }

        }

        public CloseableKvIterator<JObjectKey, JDataVersionedWrapper> getIterator(IteratorStart start, JObjectKey key) {
            return new TombstoneMergingKvIterator<>(new SnapshotKvIterator(start, key), delegateStore.getIterator(start, key));
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
