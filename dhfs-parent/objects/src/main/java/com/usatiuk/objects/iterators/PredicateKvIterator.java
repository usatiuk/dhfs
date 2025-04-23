package com.usatiuk.objects.iterators;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.function.Function;

public class PredicateKvIterator<K extends Comparable<K>, V, V_T> extends ReversibleKvIterator<K, V_T> {
    private final CloseableKvIterator<K, V> _backing;
    private final Function<V, V_T> _transformer;
    private Pair<K, V_T> _next = null;
    private boolean _checkedNext = false;

    public PredicateKvIterator(CloseableKvIterator<K, V> backing, IteratorStart start, K startKey, Function<V, V_T> transformer) {
        _goingForward = true;
        _backing = backing;
        _transformer = transformer;

        if (start == IteratorStart.GE || start == IteratorStart.GT)
            return;

        fillNext();

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
            _next = null;
            _backing.skipPrev();
            fillNext();
            _goingForward = true;
            _backing.skip();
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
//                assert _next == null || _next.getKey().compareTo(startKey) > 0;
//            }
//            case GE -> {
//                assert _next == null || _next.getKey().compareTo(startKey) >= 0;
//            }
//        }
    }

    private void fillNext() {
        while ((_goingForward ? _backing.hasNext() : _backing.hasPrev()) && _next == null) {
            var next = _goingForward ? _backing.next() : _backing.prev();
            var transformed = _transformer.apply(next.getValue());
            if (transformed == null)
                continue;
            _next = Pair.of(next.getKey(), transformed);
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

//        if (!wasAtEnd)
//            Log.tracev("Skipped in reverse: {0}", _next);

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
    protected Pair<K, V_T> nextImpl() {
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
                "_backing=" + _backing +
                ", _next=" + _next +
                '}';
    }
}
