package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.UUID;
import java.util.function.Function;

public abstract class FsNode extends JObject {
    @Getter
    final UUID _uuid;

    protected FsNode(UUID uuid) {
        this._uuid = uuid;
    }

    protected FsNode(UUID uuid, long mode) {
        this._uuid = uuid;
        this._fsNodeData._mode = mode;
    }

    @Override
    public String getName() {
        return _uuid.toString();
    }

    public static class FsNodeData implements Serializable {
        @Getter
        @Setter
        private long _mode;
    }

    final FsNodeData _fsNodeData = new FsNodeData();

    @FunctionalInterface
    public interface FsNodeFunction<R> {
        R apply(FsNodeData fsNodeData);
    }

    public <R> R runReadLocked(FsNodeFunction<R> fn) {
        lock.readLock().lock();
        try {
            return fn.apply(_fsNodeData);
        } finally {
            lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(Function<FsNodeData, R> fn) {
        lock.writeLock().lock();
        try {
            return fn.apply(_fsNodeData);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setMode(long mode) {
        runWriteLocked((fsNodeData) -> {
            fsNodeData.setMode(mode);
            return null;
        });
    }

    public long getMode() {
        return runReadLocked(FsNodeData::getMode);
    }
}
