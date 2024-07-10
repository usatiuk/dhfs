package com.usatiuk.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;

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

    private final LinkedHashSet<SetElement> _set = new LinkedHashSet<>();
    private final long _delay;

    public HashSetDelayedBlockingQueue(long delay) {
        _delay = delay;
    }

    public boolean add(T el) {
        synchronized (this) {
            if (_set.add(new SetElement(el, System.currentTimeMillis()))) {
                this.notify();
                return true;
            }
        }
        return false;
    }

    public T get() throws InterruptedException {
        while (!Thread.interrupted()) {
            long sleep;
            synchronized (this) {
                while (_set.isEmpty()) this.wait();

                var curTime = System.currentTimeMillis();

                var first = _set.getFirst()._time;
                if (first + _delay > curTime) sleep = (first + _delay) - curTime;
                else return _set.removeFirst()._el;
            }
            Thread.sleep(sleep);
        }
        throw new InterruptedException();
    }


    public Collection<T> getAll() {
        ArrayList<T> out = new ArrayList<>();

        synchronized (this) {
            var curTime = System.currentTimeMillis();

            while (!_set.isEmpty()) {
                SetElement el = _set.getFirst();
                if (el._time + _delay > curTime) break;
                out.add(_set.removeFirst()._el);
            }
        }

        return out;
    }

    public Collection<T> getAllWait() throws InterruptedException {
        ArrayList<T> out = new ArrayList<>();

        while (!Thread.interrupted()) {
            long sleep = 0;
            synchronized (this) {
                while (_set.isEmpty()) this.wait();

                var curTime = System.currentTimeMillis();

                var first = _set.getFirst()._time;
                if (first + _delay > curTime) sleep = (first + _delay) - curTime;
                else {
                    while (!_set.isEmpty()) {
                        SetElement el = _set.getFirst();
                        if (el._time + _delay > curTime) break;
                        out.add(_set.removeFirst()._el);
                    }
                }
            }
            if (sleep > 0)
                Thread.sleep(sleep);
            else return out;
        }

        throw new InterruptedException();
    }

}
