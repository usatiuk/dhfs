package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.snapshot.Snapshot;
import com.usatiuk.utils.ListUtils;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

class TransactionImpl implements Transaction, AutoCloseable {
    private final Map<JObjectKey, Optional<JDataVersionedWrapper>> _readSet = new HashMap<>();
    private final NavigableMap<JObjectKey, TxRecord.TxObjectRecord<?>> _writes = new TreeMap<>();
    private final List<Runnable> _onCommit = new LinkedList<>();
    private final List<Runnable> _onFlush = new LinkedList<>();
    private final HashSet<JObjectKey> _knownNew = new HashSet<>();
    private final Snapshot<JObjectKey, JDataVersionedWrapper> _snapshot;
    private boolean _closed = false;

    private boolean _writeTrack = false;
    private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _newWrites = new HashMap<>();

    /**
     * Identifies the source of the read: whether it's from the source or written from the transaction.
     */
    private interface ReadTrackingSourceWrapper {
        boolean fromSource();

        JData obj();
    }

    private record ReadTrackingSourceWrapperSource(JDataVersionedWrapper wrapped) implements ReadTrackingSourceWrapper {
        @Override
        public boolean fromSource() {
            return true;
        }

        @Override
        public JData obj() {
            return wrapped.data();
        }
    }

    private record ReadTrackingSourceWrapperTx(JData obj) implements ReadTrackingSourceWrapper {
        @Override
        public boolean fromSource() {
            return false;
        }
    }

    TransactionImpl(Snapshot<JObjectKey, JDataVersionedWrapper> snapshot) {
        _snapshot = snapshot;
    }

    @Override
    public void onCommit(Runnable runnable) {
        _onCommit.add(runnable);
    }

    @Override
    public void onFlush(Runnable runnable) {
        _onFlush.add(runnable);
    }

    Collection<Runnable> getOnCommit() {
        return Collections.unmodifiableCollection(_onCommit);
    }

    Snapshot<JObjectKey, JDataVersionedWrapper> snapshot() {
        return _snapshot;
    }

    Collection<Runnable> getOnFlush() {
        return Collections.unmodifiableCollection(_onFlush);
    }

    <T extends JData> Optional<T> getFromSource(Class<T> type, JObjectKey key) {
        if (_knownNew.contains(key)) {
            return Optional.empty();
        }

        return _readSet.computeIfAbsent(key, _snapshot::readObject)
                .map(JDataVersionedWrapper::data)
                .map(type::cast);
    }

    @Override
    public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key) {
        return switch (_writes.get(key)) {
            case TxRecord.TxObjectRecordWrite<?> write -> Optional.of(type.cast(write.data()));
            case TxRecord.TxObjectRecordDeleted deleted -> Optional.empty();
            case null -> getFromSource(type, key);
        };
    }

    @Override
    public void delete(JObjectKey key) {
        var record = new TxRecord.TxObjectRecordDeleted(key);
        if (_writes.put(key, record) instanceof TxRecord.TxObjectRecordDeleted) {
            return;
        }
        if (_writeTrack)
            _newWrites.put(key, record);
    }

    @Override
    public CloseableKvIterator<JObjectKey, JData> getIterator(IteratorStart start, JObjectKey key) {
        Log.tracev("Getting tx iterator with start={0}, key={1}", start, key);
        return new ReadTrackingIterator(new TombstoneSkippingIterator<JObjectKey, ReadTrackingSourceWrapper>(start, key,
                ListUtils.prependAndMap(
                        new MappingKvIterator<>(new NavigableMapKvIterator<>(_writes, start, key),
                                t -> switch (t) {
                                    case TxRecord.TxObjectRecordWrite<?> write ->
                                            new DataWrapper<ReadTrackingSourceWrapper>(new ReadTrackingSourceWrapperTx(write.data()));
                                    case TxRecord.TxObjectRecordDeleted deleted ->
                                            new TombstoneImpl<ReadTrackingSourceWrapper>();
                                    case null, default -> null;
                                }),
                        _snapshot.getIterator(start, key),
                        itin -> new MappingKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>, MaybeTombstone<ReadTrackingSourceWrapper>>(itin,
                                d -> switch (d) {
                                    case Data<JDataVersionedWrapper> w ->
                                            new DataWrapper<>(new ReadTrackingSourceWrapperSource(w.value()));
                                    case Tombstone<JDataVersionedWrapper> t -> new TombstoneImpl<>();
                                    case null, default -> null;
                                }))));
    }

    @Override
    public void put(JData obj) {
        var key = obj.key();
        var read = _readSet.get(key);
        if (read != null && (read.map(JDataVersionedWrapper::data).orElse(null) == obj)) {
            return;
        }

        var record = new TxRecord.TxObjectRecordWrite<>(obj);
        _writes.put(key, record);
        if (_writeTrack)
            _newWrites.put(key, record);
    }

    @Override
    public void putNew(JData obj) {
        var key = obj.key();
        _knownNew.add(key);

        var record = new TxRecord.TxObjectRecordWrite<>(obj);
        _writes.put(key, record);
        if (_writeTrack)
            _newWrites.put(key, record);
    }

    Collection<TxRecord.TxObjectRecord<?>> drainNewWrites() {
        if (!_writeTrack) {
            _writeTrack = true;
            return Collections.unmodifiableCollection(_writes.values());
        }
        var ret = _newWrites;
        _newWrites = new HashMap<>();
        return ret.values();
    }

    Map<JObjectKey, Optional<JDataVersionedWrapper>> reads() {
        return _readSet;
    }

    Set<JObjectKey> knownNew() {
        return _knownNew;
    }

    @Override
    public void close() {
        if (_closed) return;
        _closed = true;
        _snapshot.close();
    }

    private class ReadTrackingIterator implements CloseableKvIterator<JObjectKey, JData> {
        private final CloseableKvIterator<JObjectKey, ReadTrackingSourceWrapper> _backing;

        public ReadTrackingIterator(CloseableKvIterator<JObjectKey, ReadTrackingSourceWrapper> backing) {
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
            if (got.getValue() instanceof ReadTrackingSourceWrapperSource(JDataVersionedWrapper wrapped)) {
                _readSet.putIfAbsent(got.getKey(), Optional.of(wrapped));
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
            if (got.getValue() instanceof ReadTrackingSourceWrapperSource(JDataVersionedWrapper wrapped)) {
                _readSet.putIfAbsent(got.getKey(), Optional.of(wrapped));
            }
            return Pair.of(got.getKey(), got.getValue().obj());
        }
    }
}
