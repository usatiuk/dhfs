package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

public interface CloseableKvIterator<K extends Comparable<? super K>, V> extends Iterator<Pair<K, V>>, AutoCloseable {
    K peekNextKey();

    void skip();

    K peekPrevKey();

    Pair<K, V> prev();

    boolean hasPrev();

    void skipPrev();

    default CloseableKvIterator<K, V> reversed() {
        return new ReversedKvIterator<K, V>(this);
    }

    @Override
    void close();
}
