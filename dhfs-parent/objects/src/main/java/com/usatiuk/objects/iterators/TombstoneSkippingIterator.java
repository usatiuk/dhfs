package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class TombstoneSkippingIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private final MergingKvIterator<K, MaybeTombstone<V>> _backing;
    private Pair<K, V> _next = null;
    private boolean _checkedNext = false;

    public TombstoneSkippingIterator(IteratorStart start, K startKey, List<CloseableKvIterator<K, MaybeTombstone<V>>> iterators) {
        _goingForward = true;
        _backing = new MergingKvIterator<>(start, startKey, iterators);

        if (start == IteratorStart.GE || start == IteratorStart.GT)
            return;

        boolean shouldGoBack = false;
        if (canHaveNext())
            tryFillNext();

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
            _next = null;
            _backing.skipPrev();
            fillNext();
            _goingForward = true;
            if (_next != null)
                _backing.skip();
            fillNext();
        }
    }

    private boolean canHaveNext() {
        return (_goingForward ? _backing.hasNext() : _backing.hasPrev());
    }

    private boolean tryFillNext() {
        var next = _goingForward ? _backing.next() : _backing.prev();
        if (next.getValue() instanceof Tombstone<?>)
            return false;
        _next = Pair.of(next.getKey(), ((Data<V>) next.getValue()).value());
        return true;
    }

    private void fillNext() {
        while (_next == null && canHaveNext()) {
            tryFillNext();
        }
        _checkedNext = true;
    }

    @Override
    protected void reverse() {
        _goingForward = !_goingForward;
        boolean wasAtEnd = _next == null;

        if (_goingForward && !wasAtEnd)
            _backing.skip();
        else if (!_goingForward && !wasAtEnd)
            _backing.skipPrev();

        _next = null;
        _checkedNext = false;
    }

    @Override
    protected K peekImpl() {
        if (!_checkedNext)
            fillNext();

        if (_next == null)
            throw new NoSuchElementException();
        return _next.getKey();
    }

    @Override
    protected void skipImpl() {
        if (!_checkedNext)
            fillNext();

        if (_next == null)
            throw new NoSuchElementException();
        _next = null;
        _checkedNext = false;
    }

    @Override
    protected boolean hasImpl() {
        if (!_checkedNext)
            fillNext();

        return _next != null;
    }

    @Override
    protected Pair<K, V> nextImpl() {
        if (!_checkedNext)
            fillNext();

        if (_next == null)
            throw new NoSuchElementException("No more elements");
        var ret = _next;
        _next = null;
        _checkedNext = false;
        return ret;
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public String toString() {
        return "PredicateKvIterator{" +
                ", _next=" + _next +
                '}';
    }
}
