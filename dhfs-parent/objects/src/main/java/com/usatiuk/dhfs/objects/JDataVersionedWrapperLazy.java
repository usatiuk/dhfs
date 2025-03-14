package com.usatiuk.dhfs.objects;

import java.util.function.Supplier;

public class JDataVersionedWrapperLazy implements JDataVersionedWrapper {
    private final long _version;
    private final int _estimatedSize;
    private Supplier<JData> _producer;
    private JData _data;

    public JDataVersionedWrapperLazy(long version, int estimatedSize, Supplier<JData> producer) {
        _version = version;
        _estimatedSize = estimatedSize;
        _producer = producer;
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
