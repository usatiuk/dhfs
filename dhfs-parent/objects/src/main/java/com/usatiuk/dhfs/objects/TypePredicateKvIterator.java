package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.function.Function;

public class TypePredicateKvIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;
    private final Function<Class<?>, Boolean> _filter;
    private K _next;

    public TypePredicateKvIterator(CloseableKvIterator<K, V> backing, IteratorStart start, K startKey, Function<Class<?>, Boolean> filter) {
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


        switch (start) {
            case LT -> {
//                assert _next == null || _next.getKey().compareTo(startKey) < 0;
            }
            case LE -> {
//                assert _next == null || _next.getKey().compareTo(startKey) <= 0;
            }
            case GT -> {
                assert _next == null || _next.compareTo(startKey) > 0;
            }
            case GE -> {
                assert _next == null || _next.compareTo(startKey) >= 0;
            }
        }
    }

    private void fillNext() {
        while ((_goingForward ? _backing.hasNext() : _backing.hasPrev()) && _next == null) {
            var next = _goingForward ? _backing.peekNextType() : _backing.peekPrevType();
            if (!_filter.apply(next)) {
                if (_goingForward)
                    _backing.skip();
                else
                    _backing.skipPrev();
                continue;
            } else {
                _next = _goingForward ? _backing.peekNextKey() : _backing.peekPrevKey();
            }
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
        var nextType = _goingForward ? _backing.peekNextType() : _backing.peekPrevType();
        var got = _goingForward ? _backing.next() : _backing.prev();
        assert got.getKey().equals(retKey);
        assert nextType.equals(got.getValue().getClass());
        assert _filter.apply(got.getValue().getClass());
        fillNext();
        return got;
    }

    @Override
    protected Class<?> peekTypeImpl() {
        if (_next == null)
            throw new NoSuchElementException("No more elements");

        return _goingForward ? _backing.peekNextType() : _backing.peekPrevType();
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
