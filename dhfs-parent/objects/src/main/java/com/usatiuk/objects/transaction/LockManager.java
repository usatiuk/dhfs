package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.dhfs.utils.DataLocker;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;

@Singleton
public class LockManager {
    private final DataLocker _objLocker = new DataLocker();

    @Nonnull
    public AutoCloseableNoThrow lockObject(JObjectKey key) {
        return _objLocker.lock(key);
    }

    @Nullable
    public AutoCloseableNoThrow tryLockObject(JObjectKey key) {
        return _objLocker.tryLock(key);
    }
}
