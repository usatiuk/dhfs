package com.usatiuk.dhfs.objects;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class TombstoneMergingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;
    private final String _name;

    public TombstoneMergingKvIterator(String name, List<CloseableKvIterator<K, DataType<V>>> iterators) {
        _name = name;
        _backing = new PredicateKvIterator<>(
                new MergingKvIterator<>(name + "-merging", iterators),
                pair -> {
                    Log.tracev("{0} - Processing pair {1}", _name, pair);
                    if (pair instanceof Tombstone) {
                        return null;
                    }
                    return ((Data<V>) pair).value;
                });
    }

    @SafeVarargs
    public TombstoneMergingKvIterator(String name, CloseableKvIterator<K, DataType<V>>... iterators) {
        this(name, List.of(iterators));
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
    public void skip() {
        _backing.skip();
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

    @Override
    public String toString() {
        return "TombstoneMergingKvIterator{" +
                "_backing=" + _backing +
                ", _name='" + _name + '\'' +
                '}';
    }
}
