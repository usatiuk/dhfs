package com.usatiuk.dhfs.objects;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class InvalidatableKvIterator<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private final CloseableKvIterator<K, V> _backing;
    private final Supplier<Long> _versionSupplier;
    private final long _version;
    private final Lock _lock;

    public InvalidatableKvIterator(CloseableKvIterator<K, V> backing, Supplier<Long> versionSupplier, Lock lock) {
        _backing = backing;
        _versionSupplier = versionSupplier;
        _lock = lock;
        _version = _versionSupplier.get();
    }

    private void checkVersion() {
        if (_versionSupplier.get() != _version) {
            Log.errorv("Version mismatch: {0} != {1}", _versionSupplier.get(), _version);
            throw new InvalidIteratorException();
        }
    }

    @Override
    public K peekNextKey() {
        _lock.lock();
        try {
            checkVersion();
            return _backing.peekNextKey();
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void skip() {
        _lock.lock();
        try {
            checkVersion();
            _backing.skip();
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
        _lock.lock();
        try {
            checkVersion();
            return _backing.hasNext();
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public Pair<K, V> next() {
        _lock.lock();
        try {
            checkVersion();
            return _backing.next();
        } finally {
            _lock.unlock();
        }
    }
}
