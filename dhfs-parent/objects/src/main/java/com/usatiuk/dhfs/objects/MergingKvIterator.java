package com.usatiuk.dhfs.objects;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class MergingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final List<CloseableKvIterator<K, V>> _iterators;
    private final SortedMap<K, CloseableKvIterator<K, V>> _sortedIterators = new TreeMap<>();

    public MergingKvIterator(List<CloseableKvIterator<K, V>> iterators) {
        _iterators = iterators;

        for (CloseableKvIterator<K, V> iterator : iterators) {
            if (!iterator.hasNext()) {
                continue;
            }
            K key = iterator.peekNextKey();
            if (key != null) {
                _sortedIterators.put(key, iterator);
            }
        }
    }

    @SafeVarargs
    public MergingKvIterator(CloseableKvIterator<K, V>... iterators) {
        this(List.of(iterators));
    }

    @SafeVarargs
    public MergingKvIterator(MergingKvIterator<K, V> parent, CloseableKvIterator<K, V>... iterators) {
        this(Stream.concat(parent._iterators.stream(), Stream.of(iterators)).toList());
    }

    @Override
    public K peekNextKey() {
        var cur = _sortedIterators.pollFirstEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        return cur.getKey();
    }

    @Override
    public void close() {
        for (CloseableKvIterator<K, V> iterator : _iterators) {
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
        if (cur.getValue().hasNext()) {
            var nextKey = cur.getValue().peekNextKey();
            _sortedIterators.put(nextKey, cur.getValue());
        }
        return curVal;
    }
}
