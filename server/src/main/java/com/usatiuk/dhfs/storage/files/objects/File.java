package com.usatiuk.dhfs.storage.files.objects;

import java.util.UUID;

public class File extends FsNode {
    public File(UUID uuid) {
        super(uuid);
    }

    public File(UUID uuid, long mode) {
        super(uuid, mode);
    }

    final FileData _fileData = new FileData();

    @FunctionalInterface
    public interface FileFunction<R> {
        R apply(FsNodeData fsNodeData, FileData fileData);
    }

    public <R> R runReadLocked(FileFunction<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.apply(_fsNodeData, _fileData);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(FileFunction<R> fn) {
        _lock.writeLock().lock();
        try {
            return fn.apply(_fsNodeData, _fileData);
        } finally {
            _lock.writeLock().unlock();
        }
    }
}
