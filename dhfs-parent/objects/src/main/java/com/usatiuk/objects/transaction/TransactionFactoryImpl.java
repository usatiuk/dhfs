package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.objects.snapshot.SnapshotManager;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;

@Singleton
public class TransactionFactoryImpl implements TransactionFactory {
    @Inject
    SnapshotManager snapshotManager;
    @Inject
    LockManager lockManager;
    @ConfigProperty(name = "dhfs.objects.transaction.never-lock")
    boolean neverLock;

    @Override
    public TransactionPrivate createTransaction() {
        return new TransactionImpl();
    }

    private interface ReadTrackingInternalCrap {
        boolean fromSource();

        JData obj();
    }

    // FIXME:
    private record ReadTrackingInternalCrapSource(JDataVersionedWrapper wrapped) implements ReadTrackingInternalCrap {
        @Override
        public boolean fromSource() {
            return true;
        }

        @Override
        public JData obj() {
            return wrapped.data();
        }
    }

    private record ReadTrackingInternalCrapTx(JData obj) implements ReadTrackingInternalCrap {
        @Override
        public boolean fromSource() {
            return false;
        }
    }

    private class TransactionImpl implements TransactionPrivate {
        private final Map<JObjectKey, TransactionObject<?>> _readSet = new HashMap<>();
        private final NavigableMap<JObjectKey, TxRecord.TxObjectRecord<?>> _writes = new TreeMap<>();
        private final List<Runnable> _onCommit = new ArrayList<>();
        private final List<Runnable> _onFlush = new ArrayList<>();
        private final Snapshot<JObjectKey, JDataVersionedWrapper> _snapshot;
        private boolean _closed = false;
        private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _newWrites = new HashMap<>();

        private TransactionImpl() {
            _snapshot = snapshotManager.createSnapshot();
        }

        @Override
        public void onCommit(Runnable runnable) {
            _onCommit.add(runnable);
        }

        @Override
        public void onFlush(Runnable runnable) {
            _onFlush.add(runnable);
        }

        @Override
        public Collection<Runnable> getOnCommit() {
            return Collections.unmodifiableCollection(_onCommit);
        }

        @Override
        public Snapshot<JObjectKey, JDataVersionedWrapper> snapshot() {
            return _snapshot;
        }

        @Override
        public Collection<Runnable> getOnFlush() {
            return Collections.unmodifiableCollection(_onFlush);
        }

        @Override
        public <T extends JData> Optional<T> getFromSource(Class<T> type, JObjectKey key) {
            var got = _readSet.get(key);

            if (got == null) {
                var read = _snapshot.readObject(key);
                _readSet.put(key, new TransactionObjectNoLock<>(read));
                return read.map(JDataVersionedWrapper::data).map(type::cast);
            }

            return got.data().map(JDataVersionedWrapper::data).map(type::cast);
        }

        public <T extends JData> Optional<T> getWriteLockedFromSource(Class<T> type, JObjectKey key) {
            var got = _readSet.get(key);

            if (got == null) {
                var lock = lockManager.lockObject(key);
                try {
                    var read = _snapshot.readObject(key);
                    _readSet.put(key, new TransactionObjectLocked<>(read, lock));
                    return read.map(JDataVersionedWrapper::data).map(type::cast);
                } catch (Exception e) {
                    lock.close();
                    throw e;
                }
            }

            return got.data().map(JDataVersionedWrapper::data).map(type::cast);
        }

        @Override
        public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
            switch (_writes.get(key)) {
                case TxRecord.TxObjectRecordWrite<?> write -> {
                    return Optional.of(type.cast(write.data()));
                }
                case TxRecord.TxObjectRecordDeleted deleted -> {
                    return Optional.empty();
                }
                case null, default -> {
                }
            }

            if (neverLock)
                return getFromSource(type, key);

            return switch (strategy) {
                case OPTIMISTIC -> getFromSource(type, key);
                case WRITE -> getWriteLockedFromSource(type, key);
            };
        }

        @Override
        public void delete(JObjectKey key) {
            var got = _writes.get(key);
            if (got != null) {
                if (got instanceof TxRecord.TxObjectRecordDeleted) {
                    return;
                }
            }

            _writes.put(key, new TxRecord.TxObjectRecordDeleted(key));
            _newWrites.put(key, new TxRecord.TxObjectRecordDeleted(key));
        }

        @Override
        public CloseableKvIterator<JObjectKey, JData> getIterator(IteratorStart start, JObjectKey key) {
            Log.tracev("Getting tx iterator with start={0}, key={1}", start, key);
            return new ReadTrackingIterator(TombstoneMergingKvIterator.<JObjectKey, ReadTrackingInternalCrap>of("tx", start, key,
                    (tS, tK) -> new MappingKvIterator<>(new NavigableMapKvIterator<>(_writes, tS, tK),
                            t -> switch (t) {
                                case TxRecord.TxObjectRecordWrite<?> write ->
                                        new DataWrapper<>(new ReadTrackingInternalCrapTx(write.data()));
                                case TxRecord.TxObjectRecordDeleted deleted -> new TombstoneImpl<>();
                                case null, default -> null;
                            }),
                    (tS, tK) -> new MappingKvIterator<>(_snapshot.getIterator(tS, tK),
                            d -> new DataWrapper<ReadTrackingInternalCrap>(new ReadTrackingInternalCrapSource(d)))));
        }

        @Override
        public void put(JData obj) {
            var read = _readSet.get(obj.key());
            if (read != null && (read.data().map(JDataVersionedWrapper::data).orElse(null) == obj)) {
                return;
            }

            _writes.put(obj.key(), new TxRecord.TxObjectRecordWrite<>(obj));
            _newWrites.put(obj.key(), new TxRecord.TxObjectRecordWrite<>(obj));
        }

        @Override
        public Collection<TxRecord.TxObjectRecord<?>> drainNewWrites() {
            var ret = _newWrites;
            _newWrites = new HashMap<>();
            return ret.values();
        }

        @Override
        public Map<JObjectKey, TransactionObject<?>> reads() {
            return Collections.unmodifiableMap(_readSet);
        }

        @Override
        public void close() {
            if (_closed) return;
            _closed = true;
            _snapshot.close();
        }

        private class ReadTrackingIterator implements CloseableKvIterator<JObjectKey, JData> {
            private final CloseableKvIterator<JObjectKey, ReadTrackingInternalCrap> _backing;

            public ReadTrackingIterator(CloseableKvIterator<JObjectKey, ReadTrackingInternalCrap> backing) {
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
            public Pair<JObjectKey, JData> prev() {
                var got = _backing.prev();
                if (got.getValue() instanceof ReadTrackingInternalCrapSource(JDataVersionedWrapper wrapped)) {
                    _readSet.putIfAbsent(got.getKey(), new TransactionObjectNoLock<>(Optional.of(wrapped)));
                }
                return Pair.of(got.getKey(), got.getValue().obj());
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
            public Pair<JObjectKey, JData> next() {
                var got = _backing.next();
                if (got.getValue() instanceof ReadTrackingInternalCrapSource(JDataVersionedWrapper wrapped)) {
                    _readSet.putIfAbsent(got.getKey(), new TransactionObjectNoLock<>(Optional.of(wrapped)));
                }
                return Pair.of(got.getKey(), got.getValue().obj());
            }
        }
    }
}
