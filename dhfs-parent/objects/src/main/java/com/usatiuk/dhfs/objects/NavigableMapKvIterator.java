package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class NavigableMapKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final Iterator<Map.Entry<K, V>> _iterator;
    private Map.Entry<K, V> _next;

    public NavigableMapKvIterator(NavigableMap<K, V> map, IteratorStart start, K key) {
        SortedMap<K, V> _view;
        switch (start) {
            case GE -> _view = map.tailMap(key, true);
            case GT -> _view = map.tailMap(key, false);
            case LE -> {
                var tail = map.tailMap(key, true);
                if (tail.firstKey().equals(key)) _view = tail;
                else _view = map.tailMap(map.lowerKey(key), true);
            }
            case LT -> _view = map.tailMap(map.lowerKey(key), true);
            default -> throw new IllegalArgumentException("Unknown start type");
        }
        _iterator = _view.entrySet().iterator();
        fillNext();
    }

    private void fillNext() {
        while (_iterator.hasNext() && _next == null) {
            _next = _iterator.next();
        }
    }

    @Override
    public K peekNextKey() {
        if (_next == null) {
            throw new NoSuchElementException();
        }
        return _next.getKey();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        return _next != null;
    }

    @Override
    public Pair<K, V> next() {
        if (_next == null) {
            throw new NoSuchElementException("No more elements");
        }
        var ret = _next;
        _next = null;
        fillNext();
        return Pair.of(ret);
    }

}
