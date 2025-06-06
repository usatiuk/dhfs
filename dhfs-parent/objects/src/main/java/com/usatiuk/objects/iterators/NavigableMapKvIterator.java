package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * A key-value iterator for a {@link NavigableMap}.
 * It allows iterating over the keys and values in a sorted order.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public class NavigableMapKvIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private final NavigableMap<K, V> _map;
    private Iterator<Map.Entry<K, V>> _iterator;
    private Map.Entry<K, V> _next;

    /**
     * Constructs a NavigableMapKvIterator with the specified map, start type, and start key.
     *
     * @param map   the map to iterate over
     * @param start the starting position relative to the startKey
     * @param key   the starting key
     */
    public NavigableMapKvIterator(NavigableMap<K, ? extends V> map, IteratorStart start, K key) {
        _map = (NavigableMap<K, V>) map;
        SortedMap<K, V> _view;
        _goingForward = true;
        switch (start) {
            case GE -> _view = _map.tailMap(key, true);
            case GT -> _view = _map.tailMap(key, false);
            case LE -> {
                var floorKey = _map.floorKey(key);
                if (floorKey == null) _view = _map;
                else _view = _map.tailMap(floorKey, true);
            }
            case LT -> {
                var lowerKey = map.lowerKey(key);
                if (lowerKey == null) _view = _map;
                else _view = _map.tailMap(lowerKey, true);
            }
            default -> throw new IllegalArgumentException("Unknown start type");
        }
        _iterator = _view.entrySet().iterator();
        fillNext();
    }

    @Override
    protected void reverse() {
        var oldNext = _next;
        _next = null;
        if (_goingForward) {
            _iterator
                    = oldNext == null
                    ? _map.descendingMap().entrySet().iterator()
                    : _map.headMap(oldNext.getKey(), false).descendingMap().entrySet().iterator();
        } else {
            _iterator
                    = oldNext == null
                    ? _map.entrySet().iterator()
                    : _map.tailMap(oldNext.getKey(), false).entrySet().iterator();
        }
        _goingForward = !_goingForward;
        fillNext();
    }

    private void fillNext() {
        while (_iterator.hasNext() && _next == null) {
            _next = _iterator.next();
        }
    }

    @Override
    protected K peekImpl() {
        if (_next == null) {
            throw new NoSuchElementException();
        }
        return _next.getKey();
    }

    @Override
    protected void skipImpl() {
        if (_next == null) {
            throw new NoSuchElementException();
        }
        _next = null;
        fillNext();
    }

    @Override
    protected boolean hasImpl() {
        return _next != null;
    }

    @Override
    protected Pair<K, V> nextImpl() {
        if (_next == null) {
            throw new NoSuchElementException("No more elements");
        }
        var ret = _next;
        _next = null;
        fillNext();
        return Pair.of(ret);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "NavigableMapKvIterator{" +
                ", _next=" + _next +
                '}';
    }
}
