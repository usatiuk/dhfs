package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class SnapshotKvIterator implements CloseableKvIterator<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> {
    private final NavigableMap<SnapshotKey, SnapshotEntry> _objects;
    private final long _version;
    private final CloseableKvIterator<SnapshotKey, SnapshotEntry> _backing;
    private Pair<JObjectKey, TombstoneMergingKvIterator.DataType<JDataVersionedWrapper>> _next = null;

    public SnapshotKvIterator(NavigableMap<SnapshotKey, SnapshotEntry> objects, long version, IteratorStart start, JObjectKey startKey) {
        _objects = objects;
        _version = version;
        _backing = new NavigableMapKvIterator<>(_objects, start, new SnapshotKey(startKey, 0L));
        fillNext();
        if (_next == null) {
            return;
        }
        switch (start) {
            case LT -> {
                assert _next.getKey().compareTo(startKey) < 0;
            }
            case LE -> {
                assert _next.getKey().compareTo(startKey) <= 0;
            }
            case GT -> {
                assert _next.getKey().compareTo(startKey) > 0;
            }
            case GE -> {
                assert _next.getKey().compareTo(startKey) >= 0;
            }
        }
    }

    private void fillNext() {
        while (_backing.hasNext() && _next == null) {
            var next = _backing.next();
            var nextNextKey = _backing.hasNext() ? _backing.peekNextKey() : null;
            while (nextNextKey != null && nextNextKey.key().equals(next.getKey().key()) && nextNextKey.version() <= _version) {
                next = _backing.next();
                nextNextKey = _backing.hasNext() ? _backing.peekNextKey() : null;
            }
            // next.getValue().whenToRemove() >=_id, read tx might have same snapshot id as some write tx
            if (next.getKey().version() <= _version && next.getValue().whenToRemove() > _version) {
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
                    assert value.version() <= _version;
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
    public void skip() {
        if (_next == null)
            throw new NoSuchElementException();
        _next = null;
        fillNext();
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
            assert value.version() <= _version;
        }

        _next = null;
        fillNext();
        Log.tracev("Read: {0}, next: {1}", ret, _next);
        return ret;
    }

}
