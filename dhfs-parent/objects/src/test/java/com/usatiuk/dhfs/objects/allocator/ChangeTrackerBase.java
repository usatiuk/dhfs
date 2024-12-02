package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.ObjectAllocator;
import lombok.Getter;

public abstract class ChangeTrackerBase<T extends JData> implements ObjectAllocator.ChangeTrackingJData<T> {
    @Getter
    private boolean _modified = false;

    protected void onChange() {
        _modified = true;
    }
}
