package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class File extends DirEntry {
    public File(UUID uuid) {
        super(uuid);
    }

    @Getter
    public static class FileData implements Serializable {
        final NavigableMap<Long, String> chunks = new TreeMap<>();
    }

    final FileData fileData = new FileData();
    final ReadWriteLock lock = new ReentrantReadWriteLock();

    public <T> T runReadLocked(Function<FileData, T> fn) throws Exception {
        lock.readLock().lock();
        try {
            return fn.apply(fileData);
        } finally {
            lock.readLock().unlock();
        }
    }

    public <T> T runWriteLocked(Function<FileData, T> fn) throws Exception {
        lock.writeLock().lock();
        try {
            return fn.apply(fileData);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
