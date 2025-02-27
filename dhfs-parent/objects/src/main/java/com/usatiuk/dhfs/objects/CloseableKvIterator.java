package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

public interface CloseableKvIterator<K extends Comparable<K>, V> extends Iterator<Pair<K, V>>, AutoCloseableNoThrow {
    K peekNextKey();

    void skip();

    default K peekPrevKey() {
        throw new UnsupportedOperationException();
    }

    default Pair<K, V> prev() {
        throw new UnsupportedOperationException();
    }

    default boolean hasPrev() {
        throw new UnsupportedOperationException();
    }

    default void skipPrev() {
        throw new UnsupportedOperationException();
    }

    default CloseableKvIterator<K, V> reversed() {
        return new ReversedKvIterator<>(this);
    }
}
