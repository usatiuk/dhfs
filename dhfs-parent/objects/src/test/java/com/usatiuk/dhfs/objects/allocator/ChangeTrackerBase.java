package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.JData;
import lombok.Getter;

import java.io.Serializable;

public abstract class ChangeTrackerBase<T extends JData> implements ObjectAllocator.ChangeTrackingJData<T>, Serializable {
    @Getter
    private transient boolean _modified = false;

    protected void onChange() {
        _modified = true;
    }
}
