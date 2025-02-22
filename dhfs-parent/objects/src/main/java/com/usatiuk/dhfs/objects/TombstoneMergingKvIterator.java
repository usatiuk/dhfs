package com.usatiuk.dhfs.objects;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class TombstoneMergingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;

    public TombstoneMergingKvIterator(List<CloseableKvIterator<K, DataType<V>>> iterators) {
        _backing = new PredicateKvIterator<>(
                new MergingKvIterator<>(iterators),
                pair -> {
                    if (pair instanceof Tombstone) {
                        return null;
                    }
                    return ((Data<V>) pair).value;
                });
    }

    @SafeVarargs
    public TombstoneMergingKvIterator(CloseableKvIterator<K, DataType<V>>... iterators) {
        this(List.of(iterators));
    }

    public interface DataType<T> {
    }

    public record Tombstone<V>() implements DataType<V> {
    }

    public record Data<V>(V value) implements DataType<V> {
    }

    @Override
    public K peekNextKey() {
        return _backing.peekNextKey();
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasNext() {
        return _backing.hasNext();
    }

    @Override
    public Pair<K, V> next() {
        return _backing.next();
    }
}
