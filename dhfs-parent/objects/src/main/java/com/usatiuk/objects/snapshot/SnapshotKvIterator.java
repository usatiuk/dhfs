package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.*;
import com.usatiuk.objects.iterators.*;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;

// TODO: test me
public class SnapshotKvIterator extends ReversibleKvIterator<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> {
    private final NavigableMap<SnapshotKey, SnapshotEntry> _objects;
    private final long _version;
    private final CloseableKvIterator<SnapshotKey, SnapshotEntry> _backing;
    private Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> _next = null;

    public SnapshotKvIterator(NavigableMap<SnapshotKey, SnapshotEntry> objects, long version, IteratorStart start, JObjectKey startKey) {
        _objects = objects;
        _version = version;
        _goingForward = true;
        if (start == IteratorStart.LT || start == IteratorStart.GE)
            _backing = new NavigableMapKvIterator<>(_objects, start, new SnapshotKey(startKey, Long.MIN_VALUE));
        else if (start == IteratorStart.GT || start == IteratorStart.LE)
            _backing = new NavigableMapKvIterator<>(_objects, start, new SnapshotKey(startKey, Long.MAX_VALUE));
        else
            throw new UnsupportedOperationException();
        fill();

        boolean shouldGoBack = false;
        if (start == IteratorStart.LE) {
            if (_next == null || _next.getKey().compareTo(startKey) > 0) {
                shouldGoBack = true;
            }
        } else if (start == IteratorStart.LT) {
            if (_next == null || _next.getKey().compareTo(startKey) >= 0) {
                shouldGoBack = true;
            }
        }

        if (shouldGoBack && _backing.hasPrev()) {
            _goingForward = false;
            _backing.skipPrev();
            fill();
            _goingForward = true;
            _backing.skip();
            fill();
        }


        switch (start) {
            case LT -> {
//                assert _next == null || _next.getKey().compareTo(startKey) < 0;
            }
            case LE -> {
//                assert _next == null || _next.getKey().compareTo(startKey) <= 0;
            }
            case GT -> {
                assert _next == null || _next.getKey().compareTo(startKey) > 0;
            }
            case GE -> {
                assert _next == null || _next.getKey().compareTo(startKey) >= 0;
            }
        }

    }

    private void fillPrev(JObjectKey ltKey) {
        if (ltKey != null)
            while (_backing.hasPrev() && _backing.peekPrevKey().key().equals(ltKey)) {
                Log.tracev("Snapshot skipping prev: {0}", _backing.peekPrevKey());
                _backing.skipPrev();
            }

        _next = null;

        while (_backing.hasPrev() && _next == null) {
            var prev = _backing.prev();
            if (prev.getKey().version() <= _version && prev.getValue().whenToRemove() > _version) {
                Log.tracev("Snapshot skipping prev: {0} (too new)", prev);
                _next = switch (prev.getValue()) {
                    case SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) ->
                            Pair.of(prev.getKey().key(), new Data<>(data));
                    case SnapshotEntryDeleted(long whenToRemove) -> Pair.of(prev.getKey().key(), new Tombstone<>());
                    default -> throw new IllegalStateException("Unexpected value: " + prev.getValue());
                };
            }
        }

        if (_next != null) {
            if (_next.getValue() instanceof Data<JDataVersionedWrapper>(
                    JDataVersionedWrapper value
            )) {
                assert value.version() <= _version;
            }
        }
    }

    private void fillNext() {
        _next = null;
        while (_backing.hasNext() && _next == null) {
            var next = _backing.next();
            var nextNextKey = _backing.hasNext() ? _backing.peekNextKey() : null;
            while (nextNextKey != null && nextNextKey.key().equals(next.getKey().key()) && nextNextKey.version() <= _version) {
                Log.tracev("Snapshot skipping next: {0} (too old)", next);
                next = _backing.next();
                nextNextKey = _backing.hasNext() ? _backing.peekNextKey() : null;
            }
            // next.getValue().whenToRemove() >=_id, read tx might have same snapshot id as some write tx
            if (next.getKey().version() <= _version && next.getValue().whenToRemove() > _version) {
                _next = switch (next.getValue()) {
                    case SnapshotEntryObject(JDataVersionedWrapper data, long whenToRemove) ->
                            Pair.of(next.getKey().key(), new Data<>(data));
                    case SnapshotEntryDeleted(long whenToRemove) -> Pair.of(next.getKey().key(), new Tombstone<>());
                    default -> throw new IllegalStateException("Unexpected value: " + next.getValue());
                };
            }
            if (_next != null) {
                if (_next.getValue() instanceof Data<JDataVersionedWrapper>(
                        JDataVersionedWrapper value
                )) {
                    assert value.version() <= _version;
                }
            }
        }
    }

    private void fill() {
        if (_goingForward)
            fillNext();
        else
            fillPrev(Optional.ofNullable(_next).map(Pair::getKey).orElse(null));
    }

    @Override
    protected void reverse() {
        _goingForward = !_goingForward;

        boolean wasAtEnd = _next == null;

        if (_goingForward && !wasAtEnd)
            _backing.skip();
        else if (!_goingForward && !wasAtEnd)
            _backing.skipPrev();

        fill();
    }

    @Override
    public JObjectKey peekImpl() {
        if (_next == null)
            throw new NoSuchElementException();
        return _next.getKey();
    }

    @Override
    public void skipImpl() {
        if (_next == null)
            throw new NoSuchElementException();
        fill();
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasImpl() {
        return _next != null;
    }

    @Override
    public Pair<JObjectKey, MaybeTombstone<JDataVersionedWrapper>> nextImpl() {
        if (_next == null)
            throw new NoSuchElementException("No more elements");
        var ret = _next;
        if (ret.getValue() instanceof Data<JDataVersionedWrapper>(
                JDataVersionedWrapper value
        )) {
            assert value.version() <= _version;
        }

        fill();
        Log.tracev("Read: {0}, next: {1}", ret, _next);
        return ret;
    }

}
