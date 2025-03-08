package com.usatiuk.dhfs.utils;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.mutable.MutableObject;

import java.lang.ref.Cleaner;

public class RefcountedCloseable<T extends AutoCloseable> {
    private final T _closeable;
    private int _refCount = 1;
    private final MutableObject<Boolean> _closed = new MutableObject<>(false);
    private static final Cleaner CLEANER = Cleaner.create();

    public RefcountedCloseable(T closeable) {
        _closeable = closeable;
        var closedRef = _closed;
        CLEANER.register(this, () -> {
            if (!closedRef.getValue()) {
                Log.error("RefcountedCloseable was not closed before GC");
                System.exit(-1);
            }
        });
    }

    public RefcountedCloseable<T> ref() {
        synchronized (this) {
            if (_closed.getValue()) {
                return null;
            }
            _refCount++;
            return this;
        }
    }

    public void unref() {
        synchronized (this) {
            _refCount--;
            if (_refCount == 0) {
                try {
                    assert !_closed.getValue();
                    _closed.setValue(true);
                    _closeable.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public T get() {
        return _closeable;
    }
}
