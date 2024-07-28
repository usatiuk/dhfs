package com.usatiuk.utils;

import jakarta.annotation.Nullable;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;

public class HashSetDelayedBlockingQueue<T> {
    private record SetElement<T>(T el, long time) {
    }

    private final LinkedHashMap<T, SetElement<T>> _set = new LinkedHashMap<>();

    @Getter
    private long _delay;

    private boolean _closed = false;

    private final Object _sleepSynchronizer = new Object();

    public HashSetDelayedBlockingQueue(long delay) {
        _delay = delay;
    }

    // If there's object with key in the queue, don't do anything
    // Returns whether it was added or not
    public boolean add(T el) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");

            if (_set.containsKey(el))
                return false;

            _set.put(el, new SetElement<>(el, System.currentTimeMillis()));

            this.notify();
            return true;
        }
    }

    // Adds the object to the queue, if it exists re-adds it with a new delay
    // Returns the old object, or null
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

    // Removes the object
    public T remove(T el) {
        synchronized (this) {
            var rem = _set.remove(el);
            if (rem == null) return null;
            return rem.el();
        }
    }

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

    public T get() throws InterruptedException {
        T ret;
        do {
        } while ((ret = get(-1)) == null);
        return ret;
    }

    public boolean hasImmediate() {
        synchronized (this) {
            if (_set.isEmpty()) return false;

            var curTime = System.currentTimeMillis();

            var first = _set.firstEntry().getValue().time();
            return first + _delay <= curTime;
        }
    }

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

    public Collection<T> close() {
        synchronized (this) {
            _closed = true;
            var ret = _set.values().stream().map(SetElement::el).toList();
            _set.clear();
            return ret;
        }
    }

    public Collection<T> getAllWait() throws InterruptedException {
        Collection<T> out;
        do {
        } while ((out = getAllWait(Integer.MAX_VALUE, -1)).isEmpty());
        return out;
    }

    public Collection<T> getAllWait(int max) throws InterruptedException {
        Collection<T> out;
        do {
        } while ((out = getAllWait(max, -1)).isEmpty());
        return out;
    }

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

    public void setDelay(long delay) {
        synchronized (_sleepSynchronizer) {
            _delay = delay;
            _sleepSynchronizer.notifyAll();
        }
    }
}
