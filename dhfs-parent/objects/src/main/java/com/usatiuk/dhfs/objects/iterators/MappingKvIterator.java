package com.usatiuk.dhfs.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

import java.util.function.Function;

public class MappingKvIterator<K extends Comparable<K>, V, V_T> implements CloseableKvIterator<K, V_T> {
    private final CloseableKvIterator<K, V> _backing;
    private final Function<V, V_T> _transformer;

    public MappingKvIterator(CloseableKvIterator<K, V> backing, Function<V, V_T> transformer) {
        _backing = backing;
        _transformer = transformer;
    }

    @Override
    public K peekNextKey() {
        return _backing.peekNextKey();
    }

    @Override
    public void skip() {
        _backing.skip();
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
    public K peekPrevKey() {
        return _backing.peekPrevKey();
    }

    @Override
    public Pair<K, V_T> prev() {
        var got = _backing.prev();
        return Pair.of(got.getKey(), _transformer.apply(got.getValue()));
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
    public Pair<K, V_T> next() {
        var got = _backing.next();
        return Pair.of(got.getKey(), _transformer.apply(got.getValue()));
    }

    @Override
    public String toString() {
        return "MappingKvIterator{" +
                "_backing=" + _backing +
                '}';
    }
}
