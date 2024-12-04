package com.usatiuk.objects.alloc.runtime;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public interface ObjectAllocator {
    <T extends JData> T create(Class<T> type, JObjectKey key);

    <T extends JData> ChangeTrackingJData<T> copy(T obj);

    <T extends JData> T unmodifiable(T obj);
}
