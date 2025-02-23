package com.usatiuk.dhfs.objects;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class SelfRefreshingKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private CloseableKvIterator<K, V> _backing;
    private long _lastRefreshed = -1L;
    private Pair<K, V> _next;
    private final Object _synchronizer;
    private final Supplier<CloseableKvIterator<K, V>> _iteratorSupplier;
    private final Supplier<Long> _versionSupplier;

    public SelfRefreshingKvIterator(Supplier<CloseableKvIterator<K, V>> iteratorSupplier, Supplier<Long> versionSupplier, Object synchronizer) {
        _iteratorSupplier = iteratorSupplier;
        _versionSupplier = versionSupplier;
        _synchronizer = synchronizer;

        synchronized (_synchronizer) {
            long curVersion = _versionSupplier.get();
            _backing = _iteratorSupplier.get();
            _next = _backing.hasNext() ? _backing.next() : null;
//            if (_next != null)
//                assert _next.getValue().version() <= _id;
            _lastRefreshed = curVersion;
        }
    }

    private void doRefresh() {
        long curVersion = _versionSupplier.get();
        if (curVersion == _lastRefreshed) {
            return;
        }
        if (_next == null) return;
        synchronized (_synchronizer) {
            curVersion = _versionSupplier.get();
            Log.tracev("Refreshing iterator last refreshed {0}, current version {1}", _lastRefreshed, curVersion);
            _backing.close();
            _backing = _iteratorSupplier.get();
            var next = _backing.hasNext() ? _backing.next() : null;
            if (next == null) {
                Log.errorv("Failed to refresh iterator, null last refreshed {0}," +
                        " current version {1}, current value {2}", _lastRefreshed, curVersion, next);
                assert false;
            } else if (!next.equals(_next)) {
                Log.errorv("Failed to refresh iterator, mismatch last refreshed {0}," +
                        " current version {1}, current value {2}, read value {3}", _lastRefreshed, curVersion, _next, next);
                assert false;
            }

            _next = next;
            _lastRefreshed = curVersion;
        }
    }

    // _next should always be valid, so it's ok to do the refresh "lazily"
    private void prepareNext() {
        doRefresh();
        if (_backing.hasNext()) {
            _next = _backing.next();
//            assert _next.getValue().version() <= _id;
        } else {
            _next = null;
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
//        assert ret.getValue().version() <= _id;
        prepareNext();
        Log.tracev("Read: {0}, next: {1}", ret, _next);
        return ret;
    }

}
