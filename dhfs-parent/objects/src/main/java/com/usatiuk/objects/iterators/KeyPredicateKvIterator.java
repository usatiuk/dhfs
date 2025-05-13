package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * A key-value iterator that filters keys based on a predicate.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public class KeyPredicateKvIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;
    private final Function<K, Boolean> _filter;
    private K _next;

    /**
     * Constructs a KeyPredicateKvIterator with the specified backing iterator, start position, and filter.
     *
     * @param backing  the backing iterator
     * @param start    the starting position relative to the startKey
     * @param startKey the starting key
     * @param filter   the filter function to apply to keys. Only keys for which this function returns true will be included in the iteration.
     */
    public KeyPredicateKvIterator(CloseableKvIterator<K, V> backing, IteratorStart start, K startKey, Function<K, Boolean> filter) {
        _goingForward = true;
        _backing = backing;
        _filter = filter;
        fillNext();

        boolean shouldGoBack = false;
        if (start == IteratorStart.LE) {
            if (_next == null || _next.compareTo(startKey) > 0) {
                shouldGoBack = true;
            }
        } else if (start == IteratorStart.LT) {
            if (_next == null || _next.compareTo(startKey) >= 0) {
                shouldGoBack = true;
            }
        }

        if (shouldGoBack && _backing.hasPrev()) {
            _goingForward = false;
            _next = null;
            fillNext();
            if (_next != null)
                _backing.skipPrev();
            _goingForward = true;
//            _backing.skip();
            fillNext();
        }


//        switch (start) {
//            case LT -> {
////                assert _next == null || _next.getKey().compareTo(startKey) < 0;
//            }
//            case LE -> {
////                assert _next == null || _next.getKey().compareTo(startKey) <= 0;
//            }
//            case GT -> {
//                assert _next == null || _next.compareTo(startKey) > 0;
//            }
//            case GE -> {
//                assert _next == null || _next.compareTo(startKey) >= 0;
//            }
//        }
    }

    private void fillNext() {
        while ((_goingForward ? _backing.hasNext() : _backing.hasPrev()) && _next == null) {
            var next = _goingForward ? _backing.peekNextKey() : _backing.peekPrevKey();
            if (!_filter.apply(next)) {
                if (_goingForward)
                    _backing.skip();
                else
                    _backing.skipPrev();
                continue;
            }
            _next = next;
        }
    }

    @Override
    protected void reverse() {
        _goingForward = !_goingForward;
        _next = null;

        fillNext();
    }

    @Override
    protected K peekImpl() {
        if (_next == null)
            throw new NoSuchElementException();
        return _next;
    }

    @Override
    protected void skipImpl() {
        if (_next == null)
            throw new NoSuchElementException();
        _next = null;
        if (_goingForward)
            _backing.skip();
        else
            _backing.skipPrev();
        fillNext();
    }

    @Override
    protected boolean hasImpl() {
        return _next != null;
    }

    @Override
    protected Pair<K, V> nextImpl() {
        if (_next == null)
            throw new NoSuchElementException("No more elements");
        var retKey = _next;
        _next = null;
        var got = _goingForward ? _backing.next() : _backing.prev();
        assert got.getKey().equals(retKey);
        fillNext();
        return got;
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public String toString() {
        return "KeyPredicateKvIterator{" +
                "_backing=" + _backing +
                ", _next=" + _next +
                '}';
    }
}
