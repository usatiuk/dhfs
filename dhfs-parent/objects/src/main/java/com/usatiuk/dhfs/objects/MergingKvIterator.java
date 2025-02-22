package com.usatiuk.dhfs.objects;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class MergingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final Map<CloseableKvIterator<K, V>, Integer> _iterators;
    private final SortedMap<K, CloseableKvIterator<K, V>> _sortedIterators = new TreeMap<>();

    public MergingKvIterator(List<CloseableKvIterator<K, V>> iterators) {
        int counter = 0;
        var iteratorsTmp = new HashMap<CloseableKvIterator<K, V>, Integer>();
        for (CloseableKvIterator<K, V> iterator : iterators) {
            iteratorsTmp.put(iterator, counter++);
        }
        _iterators = Collections.unmodifiableMap(iteratorsTmp);

        for (CloseableKvIterator<K, V> iterator : iterators) {
            advanceIterator(iterator);
        }
    }

    @SafeVarargs
    public MergingKvIterator(CloseableKvIterator<K, V>... iterators) {
        this(List.of(iterators));
    }

    private void advanceIterator(CloseableKvIterator<K, V> iterator) {
        if (!iterator.hasNext()) {
            return;
        }

        K key = iterator.peekNextKey();
        if (!_sortedIterators.containsKey(key)) {
            _sortedIterators.put(key, iterator);
            return;
        }

        var oursPrio = _iterators.get(iterator);
        var them = _sortedIterators.get(key);
        var theirsPrio = _iterators.get(them);
        if (oursPrio < theirsPrio) {
            _sortedIterators.put(key, iterator);
            advanceIterator(them);
        }
    }

    @Override
    public K peekNextKey() {
        if (_sortedIterators.isEmpty())
            throw new NoSuchElementException();
        return _sortedIterators.firstKey();
    }

    @Override
    public void close() {
        for (CloseableKvIterator<K, V> iterator : _iterators.keySet()) {
            iterator.close();
        }
    }

    @Override
    public boolean hasNext() {
        return !_sortedIterators.isEmpty();
    }

    @Override
    public Pair<K, V> next() {
        var cur = _sortedIterators.pollFirstEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        var curVal = cur.getValue().next();
        advanceIterator(cur.getValue());
        return curVal;
    }
}
