package com.usatiuk.utils;

import jakarta.annotation.Nullable;
import lombok.Getter;

import java.util.*;

public class HashSetDelayedBlockingQueue<T> {
    private class SetElement {
        private final T _el;
        private final long _time;

        private SetElement(T el, long time) {
            _el = el;
            _time = time;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SetElement setElement = (SetElement) o;
            return Objects.equals(_el, setElement._el);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_el);
        }
    }

    private final LinkedHashMap<SetElement, SetElement> _set = new LinkedHashMap<>();
    private final LinkedHashSet<Thread> _waiting = new LinkedHashSet<>();
    @Getter
    private final long _delay;
    private boolean _closed = false;

    public HashSetDelayedBlockingQueue(long delay) {
        _delay = delay;
    }

    // If there's object with key in the queue, don't do anything
    // Returns whether it was added or not
    public boolean add(T el) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");
            var sel = new SetElement(el, System.currentTimeMillis());
            if (_set.put(sel, sel) == null) {
                this.notify();
                return true;
            }
        }
        return false;
    }

    // Adds the object to the queue, if it exists re-adds it with a new delay
    // Returns the old object, or null
    public T readd(T el) {
        synchronized (this) {
            if (_closed) throw new IllegalStateException("Adding to a queue that is closed!");
            var sel = new SetElement(el, System.currentTimeMillis());
            SetElement contains = _set.remove(sel);
            _set.put(sel, sel);
            this.notify();
            if (contains != null)
                return contains._el;
            else return null;
        }
    }

    // Adds the object to the queue, if it exists re-adds it with a new delay
    // Returns true if the object wasn't in the queue
    public T remove(T el) {
        synchronized (this) {
            var rem = _set.remove(new SetElement(el, 0));
            if (rem == null) return null;
            return rem._el;
        }
    }

    public T get(Long timeout) throws InterruptedException {
        long startedWaiting = System.currentTimeMillis();
        try {
            synchronized (this) {
                _waiting.add(Thread.currentThread());
            }
            while (!Thread.interrupted()) {
                long sleep;
                synchronized (this) {
                    while (_set.isEmpty()) {
                        if (timeout == null) this.wait();
                        else {
                            this.wait(timeout);
                            if (System.currentTimeMillis() > (startedWaiting + timeout)) return null;
                        }
                    }

                    var curTime = System.currentTimeMillis();

                    var first = _set.firstEntry().getValue()._time;
                    if (first + _delay > curTime) sleep = (first + _delay) - curTime;
                    else return _set.pollFirstEntry().getValue()._el;
                }
                Thread.sleep(sleep);
            }
        } finally {
            synchronized (this) {
                Thread.interrupted();
                _waiting.remove(Thread.currentThread());
            }
        }
        throw new InterruptedException();
    }

    public T get() throws InterruptedException {
        return get(null);
    }

    public T getNoDelay() throws InterruptedException {
        synchronized (this) {
            while (_set.isEmpty()) this.wait();

            return _set.pollFirstEntry().getValue()._el;
        }
    }

    public boolean hasImmediate() {
        synchronized (this) {
            if (_set.isEmpty()) return false;

            var curTime = System.currentTimeMillis();

            var first = _set.firstEntry().getValue()._time;
            return first + _delay <= curTime;
        }
    }

    @Nullable
    public T tryGet() throws InterruptedException {
        synchronized (this) {
            if (_set.isEmpty()) return null;

            var curTime = System.currentTimeMillis();

            var first = _set.firstEntry().getValue()._time;
            if (first + _delay > curTime) return null;
            else return _set.pollFirstEntry().getValue()._el;
        }
    }

    public Collection<T> getAll() {
        ArrayList<T> out = new ArrayList<>();

        synchronized (this) {
            var curTime = System.currentTimeMillis();

            while (!_set.isEmpty()) {
                SetElement el = _set.firstEntry().getValue();
                if (el._time + _delay > curTime) break;
                out.add(_set.pollFirstEntry().getValue()._el);
            }
        }

        return out;
    }

    public Collection<T> close() {
        synchronized (this) {
            _closed = true;
            var ret = _set.values().stream().map(o -> o._el).toList();
            _set.clear();
            return ret;
        }
    }

    public Collection<T> getAllWait() throws InterruptedException {
        return getAllWait(Integer.MAX_VALUE, -1);
    }

    public Collection<T> getAllWait(int max) throws InterruptedException {
        return getAllWait(max, -1);
    }

    public Collection<T> getAllWait(int max, long timeout) throws InterruptedException {
        ArrayList<T> out = new ArrayList<>();
        long startedWaiting = timeout > 0 ? System.currentTimeMillis() : -1;
        try {
            synchronized (this) {
                _waiting.add(Thread.currentThread());
            }
            while (!Thread.interrupted()) {
                if (timeout > 0)
                    if (System.currentTimeMillis() > (startedWaiting + timeout)) return out;
                long sleep = 0;
                synchronized (this) {
                    while (_set.isEmpty()) {
                        if (timeout > 0) {
                            this.wait(timeout);
                            if (System.currentTimeMillis() > (startedWaiting + timeout))
                                return out;
                        } else {
                            this.wait();
                        }
                    }

                    var curTime = System.currentTimeMillis();

                    var first = _set.firstEntry().getValue()._time;
                    if (first + _delay > curTime) sleep = (first + _delay) - curTime;
                    else {
                        while (!_set.isEmpty() && (out.size() < max)) {
                            SetElement el = _set.firstEntry().getValue();
                            if (el._time + _delay > curTime) break;
                            out.add(_set.pollFirstEntry().getValue()._el);
                        }
                    }
                }

                if (timeout > 0) {
                    var cur = System.currentTimeMillis();
                    if (cur > (startedWaiting + timeout)) return out;
                    sleep = Math.min(sleep, (startedWaiting + timeout) - cur);
                }

                if (sleep > 0)
                    Thread.sleep(sleep);
                else
                    return out;
            }
        } finally {
            synchronized (this) {
                Thread.interrupted();
                _waiting.remove(Thread.currentThread());
            }
        }
        return out;
    }

    public void interrupt() {
        synchronized (this) {
            for (var t : _waiting) t.interrupt();
        }
    }
}
