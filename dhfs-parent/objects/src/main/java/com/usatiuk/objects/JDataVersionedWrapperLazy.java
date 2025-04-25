package com.usatiuk.objects;

import java.util.function.Supplier;

public final class JDataVersionedWrapperLazy implements JDataVersionedWrapper {
    private final long _version;
    private final int _estimatedSize;
    private JData _data;
    private Supplier<JData> _producer;

    public JDataVersionedWrapperLazy(long version, int estimatedSize, Supplier<JData> producer) {
        _version = version;
        _estimatedSize = estimatedSize;
        _producer = producer;
    }

    public void setCacheCallback(Runnable cacheCallback) {
        if (_data != null) {
            throw new IllegalStateException("Cache callback can be set only before data is loaded");
        }
        var oldProducer = _producer;
        _producer = () -> {
            var ret = oldProducer.get();
            cacheCallback.run();
            return ret;
        };
    }

    public JData data() {
        if (_data != null)
            return _data;

        synchronized (this) {
            if (_data != null)
                return _data;

            _data = _producer.get();
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
