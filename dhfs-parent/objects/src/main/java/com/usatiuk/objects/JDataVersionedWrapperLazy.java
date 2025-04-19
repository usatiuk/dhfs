package com.usatiuk.objects;

import java.util.function.Supplier;

public class JDataVersionedWrapperLazy implements JDataVersionedWrapper {
    private final long _version;
    private final int _estimatedSize;
    private Supplier<JData> _producer;
    private Runnable _cacheCallback;
    private JData _data;

    public JDataVersionedWrapperLazy(long version, int estimatedSize, Supplier<JData> producer) {
        _version = version;
        _estimatedSize = estimatedSize;
        _producer = producer;
    }

    public void setCacheCallback(Runnable cacheCallback) {
        if (_data != null) {
            throw new IllegalStateException("Cache callback can be set only before data is loaded");
        }

        _cacheCallback = cacheCallback;
    }

    public JData data() {
        if (_data != null)
            return _data;

        synchronized (this) {
            if (_data != null)
                return _data;

            _data = _producer.get();
            if (_cacheCallback != null) {
                _cacheCallback.run();
                _cacheCallback = null;
            }
            _producer = null;
            return _data;
        }
    }

    public long version() {
        return _version;
    }

    @Override
    public int estimateSize() {
        return _estimatedSize;
    }
}
