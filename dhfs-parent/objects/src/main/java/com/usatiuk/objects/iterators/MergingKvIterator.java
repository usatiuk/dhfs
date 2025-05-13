package com.usatiuk.objects.iterators;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * A merging key-value iterator that combines multiple iterators into a single iterator.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public class MergingKvIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private final NavigableMap<K, IteratorEntry<K, V>> _sortedIterators = new TreeMap<>();
    private final List<IteratorEntry<K, V>> _iterators;

    /**
     * Constructs a MergingKvIterator with the specified start type, start key, and list of iterators.
     * The iterators have priority based on their order in the list: if two iterators have the same key,
     * the one that is in the beginning of the list will be used.
     *
     * @param startType the starting position relative to the startKey
     * @param startKey  the starting key
     * @param iterators the list of iterators to merge
     */
    public MergingKvIterator(IteratorStart startType, K startKey, List<CloseableKvIterator<K, V>> iterators) {
        _goingForward = true;

        {
            IteratorEntry<K, V>[] iteratorEntries = new IteratorEntry[iterators.size()];
            for (int i = 0; i < iterators.size(); i++) {
                iteratorEntries[i] = new IteratorEntry<>(i, iterators.get(i));
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

//        Log.tracev("{0} Initialized: {1}", _name, _sortedIterators);
//        switch (startType) {
////            case LT -> {
////                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) < 0;
////            }
////            case LE -> {
////                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) <= 0;
////            }
//            case GT -> {
//                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(startKey) > 0;
//            }
//            case GE -> {
//                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(startKey) >= 0;
//            }
//        }
    }

    /**
     * Constructs a MergingKvIterator with the specified start type, start key, and array of iterators.
     * The iterators have priority based on their order in the array: if two iterators have the same key,
     * the one that is in the beginning of the array will be used.
     *
     * @param startType the starting position relative to the startKey
     * @param startKey  the starting key
     * @param iterators the array of iterators to merge
     */
    @SafeVarargs
    public MergingKvIterator(IteratorStart startType, K startKey, CloseableKvIterator<K, V>... iterators) {
        this(startType, startKey, List.of(iterators));
    }

    private void advanceIterator(IteratorEntry<K, V> iteratorEntry) {
        while (iteratorEntry.iterator().hasNext()) {
            K key = iteratorEntry.iterator().peekNextKey();
//            Log.tracev("{0} Advance peeked: {1}-{2}", _name, iteratorEntry, key);

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
//                Log.tracev("{0} Skipped: {1}", _name, iteratorEntry.iterator().peekNextKey());
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
//        Log.tracev("{0} Reversing from {1}", _name, cur);
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
//        Log.tracev("{0} Skip: {1}, next: {2}", _name, cur, _sortedIterators);
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
                ", _sortedIterators=" + _sortedIterators.keySet() +
                ", _iterators=" + _iterators +
                '}';
    }

    private record IteratorEntry<K extends Comparable<K>, V>(int priority, CloseableKvIterator<K, V> iterator) {
        public IteratorEntry<K, V> reversed() {
            return new IteratorEntry<>(priority, iterator.reversed());
        }
    }
}
