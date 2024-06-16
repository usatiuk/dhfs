package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ObjectMeta implements Serializable {
    public ObjectMeta(String namespace, String name, Boolean assumeUnique) {
        this._namespace = namespace;
        this._name = name;
        this._assumeUnique = assumeUnique;
    }

    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    @Getter
    final String _namespace;
    @Getter
    final String _name;

    long _mtime;

    @Getter
    final Boolean _assumeUnique;

    //FIXME:
    final List<String> _remoteCopies = new ArrayList<>();

    public void setMtime(long mtime) {
        runWriteLocked(() -> {
            _mtime = mtime;
            return null;
        });
    }

    public long getMtime() {
        return runReadLocked(() -> _mtime);
    }

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
        _lock.writeLock().lock();
        try {
            return fn.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            _lock.writeLock().unlock();
        }
    }
}
