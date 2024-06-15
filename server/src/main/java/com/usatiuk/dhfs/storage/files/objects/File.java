package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class File extends FsNode {
    public File(UUID uuid) {
        super(uuid);
    }

    public File(UUID uuid, long mode) {
        super(uuid, mode);
    }

    @Getter
    public static class FileData implements Serializable {
        private final NavigableMap<Long, String> _chunks = new TreeMap<>();
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
