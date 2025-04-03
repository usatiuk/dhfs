package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.IteratorStart;
import org.apache.commons.collections4.MapUtils;

import java.util.*;

public class SnapshotNavigableMap<K extends Comparable<K>, V> implements NavigableMap<K, V> {
    private final Snapshot<K, V> _snapshot;
    private final K _lowerBound;
    private final boolean _lowerBoundInclusive;
    private final K _upperBound;
    private final boolean _upperBoundInclusive;
    private final boolean _descending;

    public SnapshotNavigableMap(Snapshot<K, V> snapshot,
                                K lowerBound, boolean lowerBoundInclusive,
                                K upperBound, boolean upperBoundInclusive,
                                boolean descending) {
        _snapshot = snapshot;
        _lowerBound = lowerBound;
        _lowerBoundInclusive = lowerBoundInclusive;
        _upperBound = upperBound;
        _upperBoundInclusive = upperBoundInclusive;
        _descending = descending;
    }

    private boolean isInRange(K key) {
        int cmp = key.compareTo(_lowerBound);
        if (cmp < 0 || (cmp == 0 && !_lowerBoundInclusive)) {
            return false;
        }
        cmp = key.compareTo(_upperBound);
        if (cmp > 0 || (cmp == 0 && !_upperBoundInclusive)) {
            return false;
        }
        return true;
    }

    private IteratorStart invertStart(IteratorStart iteratorStart) {
        switch (iteratorStart) {
            case LT:
                return _descending ? IteratorStart.GE : IteratorStart.LE;
            case LE:
                return _descending ? IteratorStart.GT : IteratorStart.LT;
            case GT:
                return _descending ? IteratorStart.LE : IteratorStart.GE;
            case GE:
                return _descending ? IteratorStart.LT : IteratorStart.GT;
            default:
                throw new IllegalArgumentException("Invalid iterator start: " + iteratorStart);
        }
    }

    private boolean checkCompare(IteratorStart iteratorStart, K key, K target) {
        switch (invertStart(iteratorStart)) {
            case LT:
                return key.compareTo(target) < 0;
            case LE:
                return key.compareTo(target) <= 0;
            case GT:
                return key.compareTo(target) > 0;
            case GE:
                return key.compareTo(target) >= 0;
            default:
                throw new IllegalArgumentException("Invalid iterator start: " + iteratorStart);
        }
    }

    private Entry<K, V> getEntryImpl(IteratorStart start, K key) {
        try (var it = _snapshot.getIterator(invertStart(start), key)) {
            if (!it.hasNext())
                return null;

            var nextKey = it.peekNextKey();
            if (!checkCompare(start, nextKey, key))
                return null;
            if (!isInRange(nextKey))
                return null;

            return it.next();
        }
    }

    private K getKeyImpl(IteratorStart start, K key) {
        try (var it = _snapshot.getIterator(invertStart(start), key)) {
            if (!it.hasNext())
                return null;

            var nextKey = it.peekNextKey();
            if (!checkCompare(start, nextKey, key))
                return null;
            if (!isInRange(nextKey))
                return null;

            return nextKey;
        }
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        return getEntryImpl(IteratorStart.LT, key);
    }

    @Override
    public K lowerKey(K key) {
        return getKeyImpl(IteratorStart.LT, key);
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        return getEntryImpl(IteratorStart.LE, key);
    }

    @Override
    public K floorKey(K key) {
        return getKeyImpl(IteratorStart.LE, key);
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        return getEntryImpl(IteratorStart.GE, key);
    }

    @Override
    public K ceilingKey(K key) {
        return getKeyImpl(IteratorStart.GE, key);
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        return getEntryImpl(IteratorStart.GT, key);
    }

    @Override
    public K higherKey(K key) {
        return getKeyImpl(IteratorStart.GT, key);
    }

    @Override
    public Entry<K, V> firstEntry() {
        if (_descending)
            return ceilingEntry(_upperBound);
        else
            return ceilingEntry(_lowerBound);
    }

    @Override
    public Entry<K, V> lastEntry() {
        if (_descending)
            return floorEntry(_lowerBound);
        else
            return floorEntry(_upperBound);
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<K, V> descendingMap() {
        return new SnapshotNavigableMap<>(_snapshot, _upperBound, _upperBoundInclusive, _lowerBound, _lowerBoundInclusive, !_descending);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        if(!isInRange(fromKey) || !isInRange(toKey)) {
            throw new IllegalArgumentException("Keys are out of range");
        }
        if (fromKey.compareTo(toKey) > 0) {
            throw new IllegalArgumentException("fromKey must be less than or equal to toKey");
        }

        return new SnapshotNavigableMap<>(_snapshot, fromKey, fromInclusive, toKey, toInclusive, _descending);
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        if(!isInRange(toKey)) {
            throw new IllegalArgumentException("Key is out of range");
        }
        return new SnapshotNavigableMap<>(_snapshot, _lowerBound, _lowerBoundInclusive, toKey, inclusive, _descending);
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        if(!isInRange(fromKey)) {
            throw new IllegalArgumentException("Key is out of range");
        }
        return new SnapshotNavigableMap<>(_snapshot, fromKey, inclusive, _upperBound, _upperBoundInclusive, _descending);
    }

    @Override
    public Comparator<? super K> comparator() {
        return null;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        return null;
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return null;
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return null;
    }

    @Override
    public K firstKey() {
        return null;
    }

    @Override
    public K lastKey() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return Set.of();
    }

    @Override
    public Collection<V> values() {
        return List.of();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Set.of();
    }
}
