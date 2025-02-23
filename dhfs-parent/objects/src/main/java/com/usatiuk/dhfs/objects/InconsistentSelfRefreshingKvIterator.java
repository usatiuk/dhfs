package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

// Also checks that the next provided item is always consistent after a refresh
public class InconsistentSelfRefreshingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private CloseableKvIterator<K, V> _backing;
    private long _curVersion = -1L;
    private final Lock _lock;
    private final Function<Pair<IteratorStart, K>, CloseableKvIterator<K, V>> _iteratorSupplier;
    private final Supplier<Long> _versionSupplier;
    private K _lastReturnedKey = null;
    private K _peekedKey = null;
    private boolean _peekedNext = false;
    private final Pair<IteratorStart, K> _initialStart;

    public InconsistentSelfRefreshingKvIterator(Function<Pair<IteratorStart, K>, CloseableKvIterator<K, V>> iteratorSupplier, Supplier<Long> versionSupplier, Lock lock,
                                                IteratorStart start, K key) {
        _iteratorSupplier = iteratorSupplier;
        _versionSupplier = versionSupplier;
        _lock = lock;
        _initialStart = Pair.of(start, key);

        _lock.lock();
        try {
            long curVersion = _versionSupplier.get();
            _backing = _iteratorSupplier.apply(Pair.of(start, key));
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
            oldBacking = _backing;
            if (_peekedKey != null) {
                _backing = _iteratorSupplier.apply(Pair.of(IteratorStart.GE, _peekedKey));
                if (!_backing.hasNext() || !_backing.peekNextKey().equals(_peekedKey)) {
                    throw new StaleIteratorException();
                }
            } else if (_lastReturnedKey != null) {
                _backing = _iteratorSupplier.apply(Pair.of(IteratorStart.GT, _lastReturnedKey));
            } else {
                _backing = _iteratorSupplier.apply(_initialStart);
            }

            if (_peekedNext && !_backing.hasNext()) {
                throw new StaleIteratorException();
            }

            Log.tracev("Refreshed iterator last refreshed {0}, current version {1}",
                    _curVersion, newVersion);

            _curVersion = newVersion;
        } finally {
            _lock.unlock();
            if (oldBacking != null) {
                oldBacking.close();
            }
        }
    }

    @Override
    public K peekNextKey() {
        if (_peekedKey != null) {
            return _peekedKey;
        }
        _lock.lock();
        try {
            maybeRefresh();
            _peekedKey = _backing.peekNextKey();
            assert _lastReturnedKey == null || _peekedKey.compareTo(_lastReturnedKey) > 0;
            _peekedNext = true;
            Log.tracev("Peeked key: {0}", _peekedKey);
            return _peekedKey;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void skip() {
        _lock.lock();
        try {
            maybeRefresh();
            _lastReturnedKey = _backing.peekNextKey();
            _backing.skip();
            _peekedNext = false;
            _peekedKey = null;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasNext() {
        if (_peekedNext) {
            return true;
        }
        _lock.lock();
        try {
            maybeRefresh();
            _peekedNext = _backing.hasNext();
            Log.tracev("Peeked next: {0}", _peekedNext);
            return _peekedNext;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public Pair<K, V> next() {
        _lock.lock();
        try {
            maybeRefresh();
            var got = _backing.next();
            assert _lastReturnedKey == null || got.getKey().compareTo(_lastReturnedKey) > 0;
            _peekedNext = false;
            _peekedKey = null;
            _lastReturnedKey = got.getKey();
            return got;
        } finally {
            _lock.unlock();
        }
    }

}
