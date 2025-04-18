package com.usatiuk.objects.iterators;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class MergingKvIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private record IteratorEntry<K extends Comparable<K>, V>(int priority, CloseableKvIterator<K, V> iterator) {
        public IteratorEntry<K, V> reversed() {
            return new IteratorEntry<>(priority, iterator.reversed());
        }
    }

    private final NavigableMap<K, IteratorEntry<K, V>> _sortedIterators = new TreeMap<>();
    private final String _name;
    private final List<IteratorEntry<K, V>> _iterators;

    public MergingKvIterator(String name, IteratorStart startType, K startKey, List<IterProdFn<K, V>> iterators) {
        _goingForward = true;
        _name = name;

        // Why streams are so slow?
        {
            IteratorEntry<K, V>[] iteratorEntries = new IteratorEntry[iterators.size()];
            for (int i = 0; i < iterators.size(); i++) {
                iteratorEntries[i] = new IteratorEntry<>(i, iterators.get(i).get(startType, startKey));
            }
            _iterators = List.of(iteratorEntries);
        }

        if (startType == IteratorStart.LT || startType == IteratorStart.LE) {
            // Starting at a greatest key less than/less or equal than:
            // We have a bunch of iterators that have given us theirs "greatest LT/LE key"
            // now we need to pick the greatest of those to start with
            // But if some of them don't have a lesser key, we need to pick the smallest of those

            K greatestLess = null;
            K smallestMore = null;

            for (var ite : _iterators) {
                var it = ite.iterator();
                if (it.hasNext()) {
                    var peeked = it.peekNextKey();
                    if (startType == IteratorStart.LE ? peeked.compareTo(startKey) <= 0 : peeked.compareTo(startKey) < 0) {
                        if (greatestLess == null || peeked.compareTo(greatestLess) > 0) {
                            greatestLess = peeked;
                        }
                    } else {
                        if (smallestMore == null || peeked.compareTo(smallestMore) < 0) {
                            smallestMore = peeked;
                        }
                    }
                }
            }

            K initialMaxValue;
            if (greatestLess != null)
                initialMaxValue = greatestLess;
            else
                initialMaxValue = smallestMore;

            if (initialMaxValue == null) {
                // Empty iterators
            }

            for (var ite : _iterators) {
                var iterator = ite.iterator();
                while (iterator.hasNext() && iterator.peekNextKey().compareTo(initialMaxValue) < 0) {
                    iterator.skip();
                }
            }
        }

        for (IteratorEntry<K, V> iterator : _iterators) {
            advanceIterator(iterator);
        }

        Log.tracev("{0} Initialized: {1}", _name, _sortedIterators);
        switch (startType) {
//            case LT -> {
//                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) < 0;
//            }
//            case LE -> {
//                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) <= 0;
//            }
            case GT -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(startKey) > 0;
            }
            case GE -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(startKey) >= 0;
            }
        }
    }

    @SafeVarargs
    public MergingKvIterator(String name, IteratorStart startType, K startKey, IterProdFn<K, V>... iterators) {
        this(name, startType, startKey, List.of(iterators));
    }

    private void advanceIterator(IteratorEntry<K, V> iteratorEntry) {
        while (iteratorEntry.iterator().hasNext()) {
            K key = iteratorEntry.iterator().peekNextKey();
            Log.tracev("{0} Advance peeked: {1}-{2}", _name, iteratorEntry, key);

            MutableObject<IteratorEntry<K, V>> mutableBoolean = new MutableObject<>(null);

            var newVal = _sortedIterators.merge(key, iteratorEntry, (theirsEntry, oldValOurs) -> {
                var oursPrio = oldValOurs.priority();
                var theirsPrio = theirsEntry.priority();

                if (oursPrio < theirsPrio) {
                    mutableBoolean.setValue(theirsEntry);
                    return oldValOurs;
                    // advance them
                    // return
                } else {
                    return theirsEntry;
                    // skip, continue
                }
            });

            if (newVal != iteratorEntry) {
                Log.tracev("{0} Skipped: {1}", _name, iteratorEntry.iterator().peekNextKey());
                iteratorEntry.iterator().skip();
                continue;
            }

            if (mutableBoolean.getValue() != null) {
                advanceIterator(mutableBoolean.getValue());
                return;
            }
            return;
        }
    }

    @Override
    protected void reverse() {
        var cur = _goingForward ? _sortedIterators.pollFirstEntry() : _sortedIterators.pollLastEntry();
        Log.tracev("{0} Reversing from {1}", _name, cur);
        _goingForward = !_goingForward;
        _sortedIterators.clear();
        for (IteratorEntry<K, V> iterator : _iterators) {
            // _goingForward inverted already
            advanceIterator(!_goingForward ? iterator.reversed() : iterator);
        }
        if (_sortedIterators.isEmpty() || cur == null) {
            return;
        }
        // Advance to the expected key, as we might have brought back some iterators
        // that were at their ends
        while (!_sortedIterators.isEmpty()
                && ((_goingForward && peekImpl().compareTo(cur.getKey()) <= 0)
                || (!_goingForward && peekImpl().compareTo(cur.getKey()) >= 0))) {
            skipImpl();
        }
        Log.tracev("{0} Reversed to {1}", _name, _sortedIterators);
    }

    @Override
    protected K peekImpl() {
        if (_sortedIterators.isEmpty())
            throw new NoSuchElementException();
        return _goingForward ? _sortedIterators.firstKey() : _sortedIterators.lastKey();
    }

    @Override
    protected void skipImpl() {
        var cur = _goingForward ? _sortedIterators.pollFirstEntry() : _sortedIterators.pollLastEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        cur.getValue().iterator().skip();
        advanceIterator(cur.getValue());
        Log.tracev("{0} Skip: {1}, next: {2}", _name, cur, _sortedIterators);
    }

    @Override
    protected boolean hasImpl() {
        return !_sortedIterators.isEmpty();
    }

    @Override
    protected Pair<K, V> nextImpl() {
        var cur = _goingForward ? _sortedIterators.pollFirstEntry() : _sortedIterators.pollLastEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        var curVal = cur.getValue().iterator().next();
        advanceIterator(cur.getValue());
//        Log.tracev("{0} Read from {1}: {2}, next: {3}", _name, cur.getValue(), curVal, _sortedIterators.keySet());
        return curVal;
    }

    @Override
    public void close() {
        for (IteratorEntry<K, V> iterator : _iterators) {
            iterator.iterator().close();
        }
    }

    @Override
    public String toString() {
        return "MergingKvIterator{" +
                "_name='" + _name + '\'' +
                ", _sortedIterators=" + _sortedIterators.keySet() +
                ", _iterators=" + _iterators +
                '}';
    }

    private interface FirstMatchState<K extends Comparable<K>, V> {
    }

    private record FirstMatchNone<K extends Comparable<K>, V>() implements FirstMatchState<K, V> {
    }

    private record FirstMatchFound<K extends Comparable<K>, V>(
            CloseableKvIterator<K, V> iterator) implements FirstMatchState<K, V> {
    }

    private record FirstMatchConsumed<K extends Comparable<K>, V>() implements FirstMatchState<K, V> {
    }
}
