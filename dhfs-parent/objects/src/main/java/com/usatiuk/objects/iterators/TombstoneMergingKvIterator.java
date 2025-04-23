package com.usatiuk.objects.iterators;

import io.quarkus.logging.Log;

import java.util.List;

public abstract class TombstoneMergingKvIterator {
    public static <K extends Comparable<K>, V> CloseableKvIterator<K, V> of(String name, IteratorStart startType, K startKey, List<IterProdFn<K, MaybeTombstone<V>>> iterators) {
        return new PredicateKvIterator<K, MaybeTombstone<V>, V>(
                new MergingKvIterator<K, MaybeTombstone<V>>(name + "-merging", startType, startKey, iterators),
                startType, startKey,
                pair -> {
//                    Log.tracev("{0} - Processing pair {1}", name, pair);
                    if (pair instanceof Tombstone<V>) {
                        return null;
                    }
                    return ((Data<V>) pair).value();
                });
    }

    public static <K extends Comparable<K>, V> CloseableKvIterator<K, V> of(String name, IteratorStart startType, K startKey, IterProdFn<K, MaybeTombstone<V>>... iterators) {
        return of(name, startType, startKey, List.of(iterators));
    }
}
