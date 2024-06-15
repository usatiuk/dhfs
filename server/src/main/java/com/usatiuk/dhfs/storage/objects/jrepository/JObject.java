package com.usatiuk.dhfs.storage.objects.jrepository;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class JObject implements Serializable {
    public abstract String getName();

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Serial
    private void writeObject(ObjectOutputStream oos) throws IOException {
        lock.readLock().lock();
        try {
            oos.defaultWriteObject();
        } finally {
            lock.readLock().unlock();
        }
    }
}
