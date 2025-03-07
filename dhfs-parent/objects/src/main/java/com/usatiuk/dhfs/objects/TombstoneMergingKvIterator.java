package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class TombstoneMergingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;
    private final String _name;

    public TombstoneMergingKvIterator(String name, IteratorStart startType, K startKey, List<IterProdFn<K, MaybeTombstone<V>>> iterators) {
        _name = name;
        _backing = new PredicateKvIterator<>(
                new MergingKvIterator<>(name + "-merging", startType, startKey, iterators),
                startType, startKey,
                pair -> {
                    Log.tracev("{0} - Processing pair {1}", _name, pair);
                    if (pair instanceof Tombstone) {
                        return null;
                    }
                    return ((Data<V>) pair).value();
                });
    }

    @SafeVarargs
    public TombstoneMergingKvIterator(String name, IteratorStart startType, K startKey, IterProdFn<K, MaybeTombstone<V>>... iterators) {
        this(name, startType, startKey, List.of(iterators));
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
    public K peekPrevKey() {
        return _backing.peekPrevKey();
    }

    @Override
    public Pair<K, V> prev() {
        return _backing.prev();
    }

    @Override
    public boolean hasPrev() {
        return _backing.hasPrev();
    }

    @Override
    public void skipPrev() {
        _backing.skipPrev();
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
