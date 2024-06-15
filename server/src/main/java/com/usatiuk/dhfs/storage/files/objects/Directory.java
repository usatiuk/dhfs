package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;

import java.io.Serializable;
import java.util.*;

public class Directory extends FsNode {
    public Directory(UUID uuid) {
        super(uuid);
    }

    public Directory(UUID uuid, long mode) {
        super(uuid, mode);
    }

    @Getter
    public static class DirectoryData implements Serializable {
        private final Map<String, UUID> _children = new TreeMap<>();
    }

    final DirectoryData _directoryData = new DirectoryData();

    @FunctionalInterface
    public interface DirectoryFunction<R> {
        R apply(FsNodeData fsNodeData, DirectoryData DirectoryData);
    }

    public <R> R runReadLocked(DirectoryFunction<R> fn) {
        lock.readLock().lock();
        try {
            return fn.apply(_fsNodeData, _directoryData);
        } finally {
            lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(DirectoryFunction<R> fn) {
        lock.writeLock().lock();
        try {
            return fn.apply(_fsNodeData, _directoryData);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<String, UUID> getChildrenMap() {
        return runReadLocked(((fsNodeData, directoryData) -> new TreeMap<>(directoryData.getChildren())));
    }

    public Optional<UUID> getKid(String name) {
        return runReadLocked(((fsNodeData, directoryData) -> {
            if (directoryData.getChildren().containsKey(name))
                return Optional.of(directoryData.getChildren().get(name));
            else return Optional.empty();
        }));
    }

    public boolean removeKid(String name) {
        return runWriteLocked((fsNodeData, directoryData) -> directoryData.getChildren().remove(name) != null);
    }

    public boolean putKid(String name, UUID uuid) {
        return runWriteLocked((fsNodeData, directoryData) -> {
            if (directoryData.getChildren().containsKey(name)) return false;

            directoryData.getChildren().put(name, uuid);
            return true;
        });
    }

    public List<String> getChildrenList() {
        return runReadLocked((fsNodeData, directoryData) -> directoryData.getChildren().keySet().stream().toList());
    }
}
