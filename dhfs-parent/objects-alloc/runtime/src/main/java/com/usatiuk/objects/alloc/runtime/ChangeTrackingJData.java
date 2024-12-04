package com.usatiuk.objects.alloc.runtime;

import com.usatiuk.objects.common.runtime.JData;

public interface ChangeTrackingJData<T extends JData> {
    T wrapped();

    boolean isModified();
}
