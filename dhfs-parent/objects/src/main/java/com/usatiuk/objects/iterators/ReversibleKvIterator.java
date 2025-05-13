package com.usatiuk.objects.iterators;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Base class for a reversible key-value iterator.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public abstract class ReversibleKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    /**
     * The current direction of the iterator.
     */
    protected boolean _goingForward;

    /**
     * Reverses the current direction of the iterator.
     */
    protected abstract void reverse();

    private void ensureForward() {
        if (!_goingForward) {
            reverse();
        }
    }

    private void ensureBackward() {
        if (_goingForward) {
            reverse();
        }
    }

    /**
     * Fills the next element in the iterator, depending on the current direction.
     *
     * @throws IllegalStateException if there is no next element
     */
    abstract protected K peekImpl();

    /**
     * Skips the next element in the iterator, depending on the current direction.
     *
     * @throws IllegalStateException if there is no next element
     */
    abstract protected void skipImpl();

    /**
     * Checks if there is a next element in the iterator, depending on the current direction.
     *
     * @return true if there is a next element, false otherwise
     */
    abstract protected boolean hasImpl();

    /**
     * Returns the next element in the iterator, depending on the current direction.
     *
     * @return the next element
     * @throws IllegalStateException if there is no next element
     */
    abstract protected Pair<K, V> nextImpl();

    @Override
    public K peekNextKey() {
        ensureForward();
        return peekImpl();
    }

    @Override
    public void skip() {
        ensureForward();
        skipImpl();
    }


    @Override
    public boolean hasNext() {
        ensureForward();
        return hasImpl();
    }

    @Override
    public Pair<K, V> next() {
        ensureForward();
        return nextImpl();
    }

    @Override
    public K peekPrevKey() {
        ensureBackward();
        return peekImpl();
    }

    @Override
    public Pair<K, V> prev() {
        ensureBackward();
        return nextImpl();
    }

    @Override
    public boolean hasPrev() {
        ensureBackward();
        return hasImpl();
    }

    @Override
    public void skipPrev() {
        ensureBackward();
        skipImpl();
    }

}
