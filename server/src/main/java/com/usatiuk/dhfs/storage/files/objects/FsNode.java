package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;

import java.util.UUID;
import java.util.function.Function;

public abstract class FsNode extends JObject {
    @Getter
    final UUID _uuid;

    protected FsNode(UUID uuid) {
        this._uuid = uuid;
        this._fsNodeData.setCtime(System.currentTimeMillis());
        this._fsNodeData.setMtime(this._fsNodeData.getCtime());
    }

    protected FsNode(UUID uuid, long mode) {
        this._uuid = uuid;
        this._fsNodeData.setMode(mode);
        this._fsNodeData.setCtime(System.currentTimeMillis());
        this._fsNodeData.setMtime(this._fsNodeData.getCtime());
    }

    @Override
    public String getName() {
        return _uuid.toString();
    }

    final FsNodeData _fsNodeData = new FsNodeData();

    @FunctionalInterface
    public interface FsNodeFunction<R> {
        R apply(FsNodeData fsNodeData);
    }

    public <R> R runReadLocked(FsNodeFunction<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_fsNodeData);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(Function<FsNodeData, R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.apply(_fsNodeData);
        } finally {
            _lock.writeLock().unlock();
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

    public void setCtime(long ctime) {
        runWriteLocked((fsNodeData) -> {
            fsNodeData.setCtime(ctime);
            return null;
        });
    }

    public long getCtime() {
        return runReadLocked(FsNodeData::getCtime);
    }

    public void setMtime(long mtime) {
        runWriteLocked((fsNodeData) -> {
            fsNodeData.setMtime(mtime);
            return null;
        });
    }

    public long getMtime() {
        return runReadLocked(FsNodeData::getMtime);
    }

}
