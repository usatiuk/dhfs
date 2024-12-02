package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public abstract class TestData implements JData {
    private boolean _changed = false;
    private final long _version;
    private final JObjectKey _key;

    protected TestData(long version, JObjectKey key) {
        _version = version;
        _key = key;
    }

    void onChanged() {
        _changed = true;
    }

    public boolean isChanged() {
        return _changed;
    }

    public long getVersion() {
        return _version;
    }

    @Override
    public JObjectKey getKey() {
        return _key;
    }

    public abstract TestData copy();
}
