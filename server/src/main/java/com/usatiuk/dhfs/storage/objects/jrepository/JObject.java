package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class JObject implements Serializable {
    public abstract String getName();

    protected final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public Class<? extends ConflictResolver> getConflictResolver() {
        throw new NotImplementedException();
    }

    @Serial
    private void writeObject(ObjectOutputStream oos) throws IOException {
        _lock.readLock().lock();
        try {
            oos.defaultWriteObject();
        } finally {
            _lock.readLock().unlock();
        }
    }
}
