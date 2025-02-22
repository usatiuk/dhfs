package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.dhfs.utils.DataLocker;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class LockManager {
    private final DataLocker _objLocker = new DataLocker();

    public AutoCloseableNoThrow lockObject(JObjectKey key) {
        return _objLocker.lock(key);
    }
}
