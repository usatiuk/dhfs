package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.utils.VoidFn;

public abstract class JObject<T extends JObjectData> {
    public abstract ObjectMetadata getMeta();

    public abstract T getData();

    abstract void rollback(ObjectMetadata meta, JObjectData data);

    public abstract <R> R runReadLocked(JObjectManager.ResolutionStrategy resolutionStrategy, JObjectManager.ObjectFnRead<T, R> fn);

    // Note: this is expensive
    public abstract <R> R runWriteLocked(JObjectManager.ResolutionStrategy resolutionStrategy, JObjectManager.ObjectFnWrite<T, R> fn);

    public void runReadLockedVoid(JObjectManager.ResolutionStrategy resolutionStrategy, JObjectManager.ObjectFnReadVoid<T> fn) {
        runReadLocked(resolutionStrategy, (m, d) -> {
            fn.apply(m, d);
            return null;
        });
    }

    public void runWriteLockedVoid(JObjectManager.ResolutionStrategy resolutionStrategy, JObjectManager.ObjectFnWriteVoid<T> fn) {
        runWriteLocked(resolutionStrategy, (m, d, b, v) -> {
            fn.apply(m, d, b, v);
            return null;
        });
    }

    public abstract boolean tryResolve(JObjectManager.ResolutionStrategy resolutionStrategy);

    public abstract void externalResolution(JObjectData data);

    public abstract void rwLock();

    public abstract boolean tryRwLock();

    public abstract void rwLockNoCopy();

    public abstract void rwUnlock();

    public abstract void drop();

    abstract boolean haveRwLock();

    public abstract void assertRwLock();

    public abstract void doDelete();

    public abstract void markSeen();

    public abstract void rLock();

    public abstract void rUnlock();

    public abstract void bumpVer();

    public abstract void commitFence();

    public abstract void commitFenceAsync(VoidFn callback);

    public abstract int estimateSize();

    abstract boolean updateDeletionState();
}
