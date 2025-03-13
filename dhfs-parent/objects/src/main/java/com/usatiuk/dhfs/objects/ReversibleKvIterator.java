package com.usatiuk.dhfs.objects;

import org.apache.commons.lang3.tuple.Pair;

public abstract class ReversibleKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    protected boolean _goingForward;

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

    abstract protected K peekImpl();

    abstract protected void skipImpl();

    abstract protected boolean hasImpl();

    abstract protected Pair<K, V> nextImpl();

    abstract protected Class<?> peekTypeImpl();

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

    @Override
    public Class<?> peekNextType() {
        ensureForward();
        return peekTypeImpl();
    }

    @Override
    public Class<?> peekPrevType() {
        ensureBackward();
        return peekTypeImpl();
    }
}
