package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class MergingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final Map<CloseableKvIterator<K, V>, Integer> _iterators;
    private final NavigableMap<K, CloseableKvIterator<K, V>> _sortedIterators = new TreeMap<>();
    private final String _name;

    public MergingKvIterator(String name, IteratorStart startType, K startKey, List<IterProdFn<K, V>> iterators) {
        _name = name;

        IteratorStart initialStartType = startType;
        K initialStartKey = startKey;
        boolean fail = false;
        if (startType == IteratorStart.LT || startType == IteratorStart.LE) {
            // Starting at a greatest key less than/less or equal than:
            // We have a bunch of iterators that have given us theirs "greatest LT/LE key"
            // now we need to pick the greatest of those to start with
            var initialIterators = iterators.stream().map(p -> p.get(initialStartType, initialStartKey)).toList();
            try {
                K initialMaxValue = initialIterators.stream()
                        .filter(CloseableKvIterator::hasNext)
                        .map((i) -> {
                            var peeked = i.peekNextKey();
//                            Log.warnv("peeked: {0}, from {1}", peeked, i.getClass());
                            return peeked;
                        })
                        .max(Comparator.naturalOrder()).orElse(null);
                if (initialMaxValue == null) {
                    fail = true;
                }
                startKey = initialMaxValue;
                startType = IteratorStart.GE;
            } finally {
                initialIterators.forEach(CloseableKvIterator::close);
            }
        }

        if (fail) {
            _iterators = Map.of();
            return;
        }

        int counter = 0;
        var iteratorsTmp = new HashMap<CloseableKvIterator<K, V>, Integer>();
        for (var iteratorFn : iterators) {
            var iterator = iteratorFn.get(startType, startKey);
            iteratorsTmp.put(iterator, counter++);
        }
        _iterators = Map.copyOf(iteratorsTmp);

        for (CloseableKvIterator<K, V> iterator : _iterators.keySet()) {
            advanceIterator(iterator);
        }

        Log.tracev("{0} Created: {1}", _name, _sortedIterators);
        switch (initialStartType) {
            case LT -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) < 0;
            }
            case LE -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) <= 0;
            }
            case GT -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) > 0;
            }
            case GE -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) >= 0;
            }
        }
    }

    @SafeVarargs
    public MergingKvIterator(String name, IteratorStart startType, K startKey, IterProdFn<K, V>... iterators) {
        this(name, startType, startKey, List.of(iterators));
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
        } else {
            Log.tracev("{0} Skipped: {1}", _name, iterator.peekNextKey());
            iterator.skip();
            advanceIterator(iterator);
        }
    }

    @Override
    public K peekNextKey() {
        if (_sortedIterators.isEmpty())
            throw new NoSuchElementException();
        return _sortedIterators.firstKey();
    }

    @Override
    public void skip() {
        var cur = _sortedIterators.pollFirstEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        cur.getValue().skip();
        advanceIterator(cur.getValue());
        Log.tracev("{0} Skip: {1}, next: {2}", _name, cur, _sortedIterators);
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
//        Log.tracev("{0} Read from {1}: {2}, next: {3}", _name, cur.getValue(), curVal, _sortedIterators.keySet());
        return curVal;
    }

    @Override
    public String toString() {
        return "MergingKvIterator{" +
                "_name='" + _name + '\'' +
                ", _sortedIterators=" + _sortedIterators.keySet() +
                ", _iterators=" + _iterators +
                '}';
    }
}
