package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

// Also checks that the next provided item is always consistent after a refresh
public class SelfRefreshingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private CloseableKvIterator<K, V> _backing;
    private long _curVersion = -1L;
    private final Lock _lock;
    private final Function<Pair<IteratorStart, K>, CloseableKvIterator<K, V>> _iteratorSupplier;
    private final Supplier<Long> _versionSupplier;
    private Pair<K, V> _next;

    public SelfRefreshingKvIterator(Function<Pair<IteratorStart, K>, CloseableKvIterator<K, V>> iteratorSupplier, Supplier<Long> versionSupplier, Lock lock,
                                    IteratorStart start, K key) {
        _iteratorSupplier = iteratorSupplier;
        _versionSupplier = versionSupplier;
        _lock = lock;

        _lock.lock();
        try {
            long curVersion = _versionSupplier.get();
            _backing = _iteratorSupplier.apply(Pair.of(start, key));
            _next = _backing.hasNext() ? _backing.next() : null;
            _curVersion = curVersion;
        } finally {
            _lock.unlock();
        }
    }

    private void maybeRefresh() {
        _lock.lock();
        CloseableKvIterator<K, V> oldBacking = null;
        try {
            if (_versionSupplier.get() == _curVersion) {
                return;
            }
            long newVersion = _versionSupplier.get();
            Log.tracev("Refreshing iterator last refreshed {0}, current version {1}", _curVersion, newVersion);
            oldBacking = _backing;
            _backing = _iteratorSupplier.apply(Pair.of(IteratorStart.GE, _next.getKey()));
            var next = _backing.hasNext() ? _backing.next() : null;
            if (next == null) {
                Log.errorv("Failed to refresh iterator, null last refreshed {0}," +
                        " current version {1}, current value {2}", _curVersion, newVersion, next);
                assert false;
            } else if (!next.equals(_next)) {
                Log.errorv("Failed to refresh iterator, mismatch last refreshed {0}," +
                        " current version {1}, current value {2}, read value {3}", _curVersion, newVersion, _next, next);
                assert false;
            }

            _next = next;
            _curVersion = newVersion;
        } finally {
            _lock.unlock();
            if (oldBacking != null) {
                oldBacking.close();
            }
        }
    }

    // _next should always be valid, so it's ok to do the refresh "lazily"
    private void prepareNext() {
        _lock.lock();
        try {
            maybeRefresh();
            if (_backing.hasNext()) {
                _next = _backing.next();
            } else {
                _next = null;
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public K peekNextKey() {
        if (_next == null) {
            throw new NoSuchElementException();
        }
        return _next.getKey();
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasNext() {
        return _next != null;
    }

    @Override
    public Pair<K, V> next() {
        if (_next == null) {
            throw new NoSuchElementException("No more elements");
        }
        var ret = _next;
        prepareNext();
        Log.tracev("Read: {0}, next: {1}", ret, _next);
        return ret;
    }

}
