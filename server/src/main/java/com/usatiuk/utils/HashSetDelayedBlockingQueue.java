package com.usatiuk.utils;

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

    public T get() throws InterruptedException {
        try {
            synchronized (this) {
                _waiting.add(Thread.currentThread());
            }
            while (!Thread.interrupted()) {
                long sleep;
                synchronized (this) {
                    while (_set.isEmpty()) this.wait();

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

    public T getNoDelay() throws InterruptedException {
        synchronized (this) {
            while (_set.isEmpty()) this.wait();

            return _set.pollFirstEntry().getValue()._el;
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
        ArrayList<T> out = new ArrayList<>();
        try {
            synchronized (this) {
                _waiting.add(Thread.currentThread());
            }
            while (!Thread.interrupted()) {
                long sleep = 0;
                synchronized (this) {
                    while (_set.isEmpty()) this.wait();

                    var curTime = System.currentTimeMillis();

                    var first = _set.firstEntry().getValue()._time;
                    if (first + _delay > curTime) sleep = (first + _delay) - curTime;
                    else {
                        while (!_set.isEmpty()) {
                            SetElement el = _set.firstEntry().getValue();
                            if (el._time + _delay > curTime) break;
                            out.add(_set.pollFirstEntry().getValue()._el);
                        }
                    }
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
        throw new InterruptedException();
    }

    public void interrupt() {
        synchronized (this) {
            for (var t : _waiting) t.interrupt();
        }
    }

}
