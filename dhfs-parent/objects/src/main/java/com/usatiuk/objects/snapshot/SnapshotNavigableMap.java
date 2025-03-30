package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.iterators.IteratorStart;
import jakarta.annotation.Nullable;

import java.util.*;

public class SnapshotNavigableMap<K extends Comparable<K>, V> implements NavigableMap<K, V> {

    private record Bound<K extends Comparable<K>>(K key, boolean inclusive) {
    }

    private final Snapshot<K, V> _snapshot;
    @Nullable
    private final Bound<K> _lowerBound;
    @Nullable
    private final Bound<K> _upperBound;

    private SnapshotNavigableMap(Snapshot<K, V> snapshot, Bound<K> lowerBound, Bound<K> upperBound) {
        _snapshot = snapshot;
        _lowerBound = lowerBound;
        _upperBound = upperBound;
    }

    public SnapshotNavigableMap(Snapshot<K, V> snapshot) {
        this(snapshot, null, null);
    }

    private final boolean checkBounds(K key) {
        if (_lowerBound != null) {
            if (_lowerBound.inclusive()) {
                if (key.compareTo(_lowerBound.key()) < 0) {
                    return false;
                }
            } else {
                if (key.compareTo(_lowerBound.key()) <= 0) {
                    return false;
                }
            }
        }
        if (_upperBound != null) {
            if (_upperBound.inclusive()) {
                if (key.compareTo(_upperBound.key()) > 0) {
                    return false;
                }
            } else {
                if (key.compareTo(_upperBound.key()) >= 0) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.LT, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) >= 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return it.next();
            }
        }
        return null;
    }

    @Override
    public K lowerKey(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.LT, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) >= 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return realKey;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.LE, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) > 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return it.next();
            }
        }
        return null;
    }

    @Override
    public K floorKey(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.LE, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) > 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return realKey;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.GE, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) < 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return it.next();
            }
        }
        return null;
    }

    @Override
    public K ceilingKey(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.GE, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) < 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return realKey;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.GT, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) <= 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return it.next();
            }
        }
        return null;
    }

    @Override
    public K higherKey(K key) {
        try (var it = _snapshot.getIterator(IteratorStart.GT, key)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (realKey.compareTo(key) <= 0) {
                    return null;
                }
                if (!checkBounds(realKey)) {
                    return null;
                }
                return realKey;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> firstEntry() {
        var lb = _lowerBound == null ? null : _lowerBound.key();
        var start = _lowerBound != null ? (_lowerBound.inclusive() ? IteratorStart.GE : IteratorStart.GT) : IteratorStart.GE;
        try (var it = _snapshot.getIterator(start, lb)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (!checkBounds(realKey)) {
                    return null;
                }
                return it.next();
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> lastEntry() {
        var b = _upperBound == null ? null : _upperBound.key();
        var start = _upperBound != null ? (_upperBound.inclusive() ? IteratorStart.LE : IteratorStart.LT) : IteratorStart.LE;
        try (var it = _snapshot.getIterator(start, b)) {
            if (it.hasNext()) {
                var realKey = it.peekNextKey();
                if (!checkBounds(realKey)) {
                    return null;
                }
                return it.next();
            }
        }
        return null;
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
        return null;
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return null;
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return null;
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return null;
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return null;
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return null;
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
