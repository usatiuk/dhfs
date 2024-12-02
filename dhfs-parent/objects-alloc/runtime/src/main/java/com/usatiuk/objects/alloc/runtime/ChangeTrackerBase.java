package com.usatiuk.objects.alloc.runtime;

import com.usatiuk.objects.common.JData;

import java.io.Serializable;

abstract class ChangeTrackerBase<T extends JData> implements ObjectAllocator.ChangeTrackingJData<T>, Serializable {
    private transient boolean _modified = false;

    public boolean isModified() {
        return _modified;
    }

    protected void onChange() {
        _modified = true;
    }
}
