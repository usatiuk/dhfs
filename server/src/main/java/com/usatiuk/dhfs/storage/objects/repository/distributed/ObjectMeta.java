package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ObjectMeta implements Serializable {
    public ObjectMeta(String namespace, String name) {
        this._namespace = namespace;
        this._name = name;
    }

    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    @Getter
    final String _namespace;
    @Getter
    final String _name;

    @Getter
    @Setter
    long _mtime;

    @Getter
    final List<HostInfo> _remoteCopies = new ArrayList<>();

    public <R> R runReadLocked(Callable<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            _lock.readLock().unlock();
        }
    }

    public <R> R runWriteLocked(Callable<R> fn) {
        _lock.readLock().lock();
        try {
            return fn.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            _lock.readLock().unlock();
        }
    }
}
