package com.usatiuk.dhfs.objects;

import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.function.Function;

public class PredicateKvIterator<K extends Comparable<K>, V, V_T> implements CloseableKvIterator<K, V_T> {
    private final CloseableKvIterator<K, V> _backing;
    private final Function<V, V_T> _transformer;
    private Pair<K, V_T> _next;

    public PredicateKvIterator(CloseableKvIterator<K, V> backing, Function<V, V_T> transformer) {
        _backing = backing;
        _transformer = transformer;
        fillNext();
    }

    private void fillNext() {
        while (_backing.hasNext() && _next == null) {
            var next = _backing.next();
            var transformed = _transformer.apply(next.getValue());
            if (transformed == null)
                continue;
            _next = Pair.of(next.getKey(), transformed);
        }
    }

    @Override
    public K peekNextKey() {
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
    public Pair<K, V_T> next() {
        if (_next == null)
            throw new NoSuchElementException("No more elements");
        var ret = _next;
        _next = null;
        fillNext();
        return ret;
    }

    @Override
    public String toString() {
        return "PredicateKvIterator{" +
                "_backing=" + _backing +
                ", _next=" + _next +
                '}';
    }
}
