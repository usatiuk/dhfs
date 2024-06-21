package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class JObject implements Serializable {
    public abstract String getName();

    protected final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public Class<? extends ConflictResolver> getConflictResolver() {
        throw new NotImplementedException();
    }
}
