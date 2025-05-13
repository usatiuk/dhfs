package com.usatiuk.utils;

import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.function.Function;

/**
 * Blocking queue that delays the objects for a given time, and deduplicates them.
 *
 * @param <T> the type of the objects in the queue
 */
public class HashSetDelayedBlockingQueue<T> {
    private final LinkedHashMap<T, SetElement<T>> _set = new LinkedHashMap<>();
    private final Object _sleepSynchronizer = new Object();
    private long _delay;
    private boolean _closed = false;

    /**
     * Creates a new HashSetDelayedBlockingQueue with the specified delay.
     *
     * @param delay the delay in milliseconds
     */
    public HashSetDelayedBlockingQueue(long delay) {
        _delay = delay;
    }

    /**
     * @return the delay in milliseconds
     */
    public long getDelay() {
        return _delay;
    }

    /**
     * Sets the delay for the queue.
     *
     * @param delay the delay in milliseconds
     */
    public void setDelay(long delay) {
        synchronized (_sleepSynchronizer) {
            _delay = delay;
            _sleepSynchronizer.notifyAll();
        }
    }

    /**
     * Adds the object to the queue if it doesn't exist.
     *
     * @param el the object to add
     * @return true if the object was added, false if it already exists
     */
    public boolean add(T el) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");

            if (_set.putIfAbsent(el, new SetElement<>(el, System.currentTimeMillis())) != null)
                return false;

            this.notify();
            return true;
        }
    }


    /**
     * Adds the object to the queue with no delay.
     *
     * @param el the object to add
     * @return the old object if it existed, null otherwise
     */
    public T addNoDelay(T el) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");

            SetElement<T> old = _set.putFirst(el, new SetElement<>(el, 0));
            this.notify();

            if (old != null)
                return old.el();
            else
                return null;
        }
    }

    /**
     * Adds the object to the queue, if it exists re-adds it with a new delay
     *
     * @param el the object to add
     * @return the old object if it existed, null otherwise
     */
    public T readd(T el) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");

            SetElement<T> old = _set.putLast(el, new SetElement<>(el, System.currentTimeMillis()));
            this.notify();

            if (old != null)
                return old.el();
            else
                return null;
        }
    }

    /**
     * Merges the object with the old one.
     *
     * @param el          the object to merge
     * @param transformer the function to transform the old object
     * @return the old object if it existed, null otherwise
     */
    public T merge(T el, Function<T, T> transformer) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");

            var old = _set.get(el);

            var next = new SetElement<>(transformer.apply(old != null ? old.el : null), System.currentTimeMillis());

            _set.putLast(el, next);

            this.notify();

            if (old != null)
                return old.el();
            else
                return null;
        }
    }

    /**
     * Removes the object from the queue.
     *
     * @param el the object to remove
     * @return the removed object, or null if it didn't exist
     */
    public T remove(T el) {
        synchronized (this) {
            var rem = _set.remove(el);
            if (rem == null) return null;
            return rem.el();
        }
    }

    /**
     * Gets the object from the queue, waiting for it if necessary.
     *
     * @param timeout the timeout in milliseconds, or -1 for no timeout
     * @return the object, or null if it timed out
     * @throws InterruptedException if the thread is interrupted
     */
    public T get(long timeout) throws InterruptedException {
        long startedWaiting = timeout > 0 ? System.currentTimeMillis() : -1;

        while (!Thread.interrupted()) {
            long sleep;
            synchronized (this) {
                if (timeout > 0)
                    if (System.currentTimeMillis() > (startedWaiting + timeout)) return null;

                while (_set.isEmpty()) {
                    if (timeout > 0) {
                        this.wait(Math.max(timeout - (System.currentTimeMillis() - startedWaiting), 1));
                        if (System.currentTimeMillis() > (startedWaiting + timeout)) return null;
                    } else {
                        this.wait();
                    }
                }

                var curTime = System.currentTimeMillis();

                var first = _set.firstEntry().getValue().time();

                if (first + _delay > curTime)
                    sleep = (first + _delay) - curTime;
                else
                    return _set.pollFirstEntry().getValue().el();
            }

            if (timeout > 0)
                sleep = Math.min(sleep, (startedWaiting + timeout) - System.currentTimeMillis());

            if (sleep <= 0)
                continue;

            synchronized (_sleepSynchronizer) {
                _sleepSynchronizer.wait(sleep);
            }
        }

        throw new InterruptedException();
    }

    /**
     * Gets the object from the queue, waiting for it if necessary.
     *
     * @return the object
     * @throws InterruptedException if the thread is interrupted
     */
    public T get() throws InterruptedException {
        T ret;
        do {
        } while ((ret = get(-1)) == null);
        return ret;
    }

    /**
     * Checks if the queue has an object that is ready to be processed.
     *
     * @return true if there is an object ready, false otherwise
     */
    public boolean hasImmediate() {
        synchronized (this) {
            if (_set.isEmpty()) return false;

            var curTime = System.currentTimeMillis();

            var first = _set.firstEntry().getValue().time();
            return first + _delay <= curTime;
        }
    }

    /**
     * Tries to get the object from the queue without waiting.
     *
     * @return the object, or null if it doesn't exist
     */
    @Nullable
    public T tryGet() {
        synchronized (this) {
            if (_set.isEmpty()) return null;

            var curTime = System.currentTimeMillis();

            var first = _set.firstEntry().getValue().time();

            if (first + _delay > curTime)
                return null;
            else
                return _set.pollFirstEntry().getValue().el();
        }
    }

    /**
     * Gets all objects from the queue that are ready to be processed.
     *
     * @return a collection of objects
     */
    public Collection<T> getAll() {
        ArrayList<T> out = new ArrayList<>();

        synchronized (this) {
            var curTime = System.currentTimeMillis();

            while (!_set.isEmpty()) {
                SetElement<T> el = _set.firstEntry().getValue();
                if (el.time() + _delay > curTime) break;
                out.add(_set.pollFirstEntry().getValue().el());
            }
        }

        return out;
    }

    /**
     * Closes the queue and returns all objects in it.
     *
     * @return a collection of objects
     */
    public Collection<T> close() {
        synchronized (this) {
            _closed = true;
            var ret = _set.values().stream().map(SetElement::el).toList();
            _set.clear();
            return ret;
        }
    }

    /**
     * Gets all objects from the queue, waiting for them if necessary.
     *
     * @return a collection of objects
     * @throws InterruptedException if the thread is interrupted
     */
    public Collection<T> getAllWait() throws InterruptedException {
        Collection<T> out;
        do {
        } while ((out = getAllWait(Integer.MAX_VALUE, -1)).isEmpty());
        return out;
    }

    /**
     * Gets all objects from the queue, waiting for them if necessary.
     *
     * @param max the maximum number of objects to get
     * @return a collection of objects
     * @throws InterruptedException if the thread is interrupted
     */
    public Collection<T> getAllWait(int max) throws InterruptedException {
        Collection<T> out;
        do {
        } while ((out = getAllWait(max, -1)).isEmpty());
        return out;
    }

    /**
     * Gets all objects from the queue, waiting for them if necessary.
     *
     * @param max     the maximum number of objects to get
     * @param timeout the timeout in milliseconds, or -1 for no timeout
     * @return a collection of objects
     * @throws InterruptedException if the thread is interrupted
     */
    public Collection<T> getAllWait(int max, long timeout) throws InterruptedException {
        ArrayList<T> out = new ArrayList<>();

        long startedWaiting = timeout > 0 ? System.currentTimeMillis() : -1;

        while (!Thread.interrupted()) {
            if (timeout > 0)
                if (System.currentTimeMillis() > (startedWaiting + timeout)) return out;

            long sleep = 0;

            synchronized (this) {
                while (_set.isEmpty()) {
                    if (timeout > 0) {
                        this.wait(Math.max(timeout - (System.currentTimeMillis() - startedWaiting), 1));
                        if (System.currentTimeMillis() > (startedWaiting + timeout))
                            return out;
                    } else {
                        this.wait();
                    }
                }

                var curTime = System.currentTimeMillis();

                var first = _set.firstEntry().getValue().time();
                if (first + _delay > curTime)
                    sleep = (first + _delay) - curTime;
                else {
                    while (!_set.isEmpty() && (out.size() < max)) {
                        SetElement<T> el = _set.firstEntry().getValue();
                        if (el.time() + _delay > curTime) break;
                        out.add(_set.pollFirstEntry().getValue().el());
                    }
                }
            }

            if (timeout > 0) {
                var cur = System.currentTimeMillis();
                if (cur > (startedWaiting + timeout)) return out;
                sleep = Math.min(sleep, (startedWaiting + timeout) - cur);
            }

            if (sleep > 0) {
                synchronized (_sleepSynchronizer) {
                    _sleepSynchronizer.wait(sleep);
                }
            } else
                return out;
        }

        return out;
    }

    private record SetElement<T>(T el, long time) {
    }
}
