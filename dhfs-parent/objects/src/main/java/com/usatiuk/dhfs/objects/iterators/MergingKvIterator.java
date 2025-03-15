package com.usatiuk.dhfs.objects.iterators;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class MergingKvIterator<K extends Comparable<K>, V> extends ReversibleKvIterator<K, V> {
    private final NavigableMap<K, CloseableKvIterator<K, V>> _sortedIterators = new TreeMap<>();
    private final String _name;
    private Map<CloseableKvIterator<K, V>, Integer> _iterators;

    private final IteratorStart _initialStartType;
    private final K _initialStartKey;

    private interface FirstMatchState<K extends Comparable<K>, V> {
    }

    private record FirstMatchNone<K extends Comparable<K>, V>() implements FirstMatchState<K, V> {
    }

    private record FirstMatchFound<K extends Comparable<K>, V>(
            CloseableKvIterator<K, V> iterator) implements FirstMatchState<K, V> {
    }

    private record FirstMatchConsumed<K extends Comparable<K>, V>() implements FirstMatchState<K, V> {
    }

    // Fast path for the first element
    private FirstMatchState<K, V> _firstMatchState;
    private final List<IterProdFn<K, V>> _pendingIterators;

    public MergingKvIterator(String name, IteratorStart startType, K startKey, List<IterProdFn<K, V>> iterators) {
        _goingForward = true;
        _name = name;
        _initialStartType = startType;
        _initialStartKey = startKey;

        {
            int counter = 0;
            var iteratorsTmp = new HashMap<CloseableKvIterator<K, V>, Integer>();
            for (var iteratorFn : iterators) {
                var iterator = iteratorFn.get(startType, startKey);
                if ((counter == 0) // Not really a requirement but simplifies some things for now
                        && (startType == IteratorStart.GE || startType == IteratorStart.LE)
                        && iterator.hasNext()
                        && iterator.peekNextKey().equals(startKey)) {
                    _firstMatchState = new FirstMatchFound<>(iterator);
                    _pendingIterators = iterators;
                    Log.tracev("{0} Created fast match: {1}", _name, _firstMatchState);
                    return;
                }
                iteratorsTmp.put(iterator, counter++);
            }
            _iterators = Map.copyOf(iteratorsTmp);
            _pendingIterators = null;
        }

        _firstMatchState = new FirstMatchNone<>();
        doInitialAdvance();
    }

    private void doInitialAdvance() {
        if (_initialStartType == IteratorStart.LT || _initialStartType == IteratorStart.LE) {
            // Starting at a greatest key less than/less or equal than:
            // We have a bunch of iterators that have given us theirs "greatest LT/LE key"
            // now we need to pick the greatest of those to start with
            // But if some of them don't have a lesser key, we need to pick the smallest of those
            var found = _iterators.keySet().stream()
                    .filter(CloseableKvIterator::hasNext)
                    .map((i) -> {
                        var peeked = i.peekNextKey();
//                            Log.warnv("peeked: {0}, from {1}", peeked, i.getClass());
                        return peeked;
                    }).distinct().collect(Collectors.partitioningBy(e -> _initialStartType == IteratorStart.LE ? e.compareTo(_initialStartKey) <= 0 : e.compareTo(_initialStartKey) < 0));
            K initialMaxValue;
            if (!found.get(true).isEmpty())
                initialMaxValue = found.get(true).stream().max(Comparator.naturalOrder()).orElse(null);
            else
                initialMaxValue = found.get(false).stream().min(Comparator.naturalOrder()).orElse(null);

            for (var iterator : _iterators.keySet()) {
                while (iterator.hasNext() && iterator.peekNextKey().compareTo(initialMaxValue) < 0) {
                    iterator.skip();
                }
            }
        }

        for (CloseableKvIterator<K, V> iterator : _iterators.keySet()) {
            advanceIterator(iterator);
        }

        Log.tracev("{0} Initialized: {1}", _name, _sortedIterators);
        switch (_initialStartType) {
//            case LT -> {
//                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) < 0;
//            }
//            case LE -> {
//                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(initialStartKey) <= 0;
//            }
            case GT -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(_initialStartKey) > 0;
            }
            case GE -> {
                assert _sortedIterators.isEmpty() || _sortedIterators.firstKey().compareTo(_initialStartKey) >= 0;
            }
        }
    }

    private void doHydrate() {
        if (_firstMatchState instanceof FirstMatchNone) {
            return;
        }

        boolean consumed = _firstMatchState instanceof FirstMatchConsumed;
        if (_firstMatchState instanceof FirstMatchFound(CloseableKvIterator iterator)) {
            iterator.close();
        }

        _firstMatchState = new FirstMatchNone<>();

        {
            int counter = 0;
            var iteratorsTmp = new HashMap<CloseableKvIterator<K, V>, Integer>();
            for (var iteratorFn : _pendingIterators) {
                var iterator = iteratorFn.get(consumed ? IteratorStart.GT : IteratorStart.GE, _initialStartKey);
                iteratorsTmp.put(iterator, counter++);
            }
            _iterators = Map.copyOf(iteratorsTmp);
        }

        doInitialAdvance();
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
        Log.tracev("{0} Advance peeked: {1}-{2}", _name, iterator, key);
        if (!_sortedIterators.containsKey(key)) {
            _sortedIterators.put(key, iterator);
            return;
        }

        // Expects that reversed iterator returns itself when reversed again
        var oursPrio = _iterators.get(_goingForward ? iterator : iterator.reversed());
        var them = _sortedIterators.get(key);
        var theirsPrio = _iterators.get(_goingForward ? them : them.reversed());
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
    protected void reverse() {
        switch (_firstMatchState) {
            case FirstMatchFound<K, V> firstMatchFound -> {
                doHydrate();
            }
            case FirstMatchConsumed<K, V> firstMatchConsumed -> {
                doHydrate();
            }
            default -> {
            }
        }

        var cur = _goingForward ? _sortedIterators.pollFirstEntry() : _sortedIterators.pollLastEntry();
        Log.tracev("{0} Reversing from {1}", _name, cur);
        _goingForward = !_goingForward;
        _sortedIterators.clear();
        for (CloseableKvIterator<K, V> iterator : _iterators.keySet()) {
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
        switch (_firstMatchState) {
            case FirstMatchFound<K, V> firstMatchFound -> {
                return firstMatchFound.iterator.peekNextKey();
            }
            case FirstMatchConsumed<K, V> firstMatchConsumed -> {
                doHydrate();
                break;
            }
            default -> {
            }
        }

        if (_sortedIterators.isEmpty())
            throw new NoSuchElementException();
        return _goingForward ? _sortedIterators.firstKey() : _sortedIterators.lastKey();
    }

    @Override
    protected void skipImpl() {
        switch (_firstMatchState) {
            case FirstMatchFound<K, V> firstMatchFound -> {
                var curVal = firstMatchFound.iterator.next();
                firstMatchFound.iterator.close();
                _firstMatchState = new FirstMatchConsumed<>();
//                Log.tracev("{0} Read from {1}: {2}, next: {3}", _name, firstMatchFound.iterator, curVal, _sortedIterators.keySet());
                return;
            }
            case FirstMatchConsumed<K, V> firstMatchConsumed -> {
                doHydrate();
                break;
            }
            default -> {
            }
        }

        var cur = _goingForward ? _sortedIterators.pollFirstEntry() : _sortedIterators.pollLastEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        cur.getValue().skip();
        advanceIterator(cur.getValue());
        Log.tracev("{0} Skip: {1}, next: {2}", _name, cur, _sortedIterators);
    }

    @Override
    protected boolean hasImpl() {
        switch (_firstMatchState) {
            case FirstMatchFound<K, V> firstMatchFound -> {
                return true;
            }
            case FirstMatchConsumed<K, V> firstMatchConsumed -> {
                doHydrate();
                break;
            }
            default -> {
            }
        }
        return !_sortedIterators.isEmpty();
    }

    @Override
    protected Pair<K, V> nextImpl() {
        switch (_firstMatchState) {
            case FirstMatchFound<K, V> firstMatchFound -> {
                var curVal = firstMatchFound.iterator.next();
                firstMatchFound.iterator.close();
                _firstMatchState = new FirstMatchConsumed<>();
//                Log.tracev("{0} Read from {1}: {2}, next: {3}", _name, firstMatchFound.iterator, curVal, _sortedIterators.keySet());
                return curVal;
            }
            case FirstMatchConsumed<K, V> firstMatchConsumed -> {
                doHydrate();
                break;
            }
            default -> {
            }
        }

        var cur = _goingForward ? _sortedIterators.pollFirstEntry() : _sortedIterators.pollLastEntry();
        if (cur == null) {
            throw new NoSuchElementException();
        }
        var curVal = cur.getValue().next();
        advanceIterator(cur.getValue());
//        Log.tracev("{0} Read from {1}: {2}, next: {3}", _name, cur.getValue(), curVal, _sortedIterators.keySet());
        return curVal;
    }


    @Override
    public void close() {
        if (_firstMatchState instanceof FirstMatchFound(CloseableKvIterator iterator)) {
            iterator.close();
        }
        for (CloseableKvIterator<K, V> iterator : _iterators.keySet()) {
            iterator.close();
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
}
