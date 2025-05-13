package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;


/**
 * An iterator over key-value pairs that can be closed and supports peek and skip operations, in both directions.
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public interface CloseableKvIterator<K extends Comparable<? super K>, V> extends Iterator<Pair<K, V>>, AutoCloseable {
    /**
     * Returns the upcoming key in the forward direction without advancing the iterator.
     *
     * @return the current key
     * @throws IllegalStateException if there is no next element
     */
    K peekNextKey();

    /**
     * Skips the next element in the forward direction.
     *
     * @throws IllegalStateException if there is no next element
     */
    void skip();

    /**
     * Checks if there is a next element in the forward direction.
     *
     * @return true if there is a next element, false otherwise
     * @throws IllegalStateException if there is no next element
     */
    K peekPrevKey();

    /**
     * Returns the key-value pair in the reverse direction, and advances the iterator.
     *
     * @return the previous key-value pair
     * @throws IllegalStateException if there is no previous element
     */
    Pair<K, V> prev();

    /**
     * Checks if there is a previous element in the reverse direction.
     *
     * @return true if there is a previous element, false otherwise
     */
    boolean hasPrev();

    /**
     * Skips the previous element in the reverse direction.
     *
     * @throws IllegalStateException if there is no previous element
     */
    void skipPrev();

    /**
     * Returns a reversed iterator that iterates in the reverse direction.
     *
     * @return a new CloseableKvIterator that iterates in the reverse direction
     */
    default CloseableKvIterator<K, V> reversed() {
        return new ReversedKvIterator<K, V>(this);
    }

    @Override
    void close();
}
