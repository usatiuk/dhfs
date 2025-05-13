package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

/**
 * A wrapper for a key-value iterator that iterates in reverse order.
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public class ReversedKvIterator<K extends Comparable<? super K>, V> implements CloseableKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;

    /**
     * Constructs a ReversedKvIterator with the specified backing iterator.
     *
     * @param backing the backing iterator
     */
    public ReversedKvIterator(CloseableKvIterator<K, V> backing) {
        _backing = backing;
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasNext() {
        return _backing.hasPrev();
    }

    @Override
    public Pair<K, V> next() {
        return _backing.prev();
    }

    @Override
    public K peekNextKey() {
        return _backing.peekPrevKey();
    }

    @Override
    public void skip() {
        _backing.skipPrev();
    }

    @Override
    public K peekPrevKey() {
        return _backing.peekNextKey();
    }

    @Override
    public Pair<K, V> prev() {
        return _backing.next();
    }

    @Override
    public boolean hasPrev() {
        return _backing.hasNext();
    }

    @Override
    public void skipPrev() {
        _backing.skip();
    }

    @Override
    public CloseableKvIterator<K, V> reversed() {
        return _backing;
    }
}
