package com.usatiuk.objects.alloc.runtime;

import com.usatiuk.objects.common.JData;
import com.usatiuk.objects.common.JObjectKey;

public interface ObjectAllocator {
    <T extends JData> T create(Class<T> type, JObjectKey key);

    interface ChangeTrackingJData<T extends JData> {
        T wrapped();

        boolean isModified();
    }

    // A copy of data that can be modified without affecting the original, and that can track changes
    <T extends JData> ChangeTrackingJData<T> copy(T obj);

    <T extends JData> T unmodifiable(T obj);
}
