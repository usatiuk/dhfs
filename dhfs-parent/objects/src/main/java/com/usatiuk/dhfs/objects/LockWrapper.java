package com.usatiuk.dhfs.objects;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockWrapper<T extends JData> {
    private final JData _data;
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

    public LockWrapper(T data) {
        _data = data;
    }

    public boolean sameObject(JData data) {
        return _data == data;
    }

    interface DataAccessor<T extends JData> extends AutoCloseable {
        T getData();
    }

    public class ReadLocked<B extends JData> implements DataAccessor<B> {
        public ReadLocked() {
            _lock.readLock().lock();
        }

        @Override
        public void close() {
            _lock.readLock().unlock();
        }

        @Override
        public B getData() {
            return (B) _data;
        }
    }

    public ReadLocked<T> read() {
        return new ReadLocked<>();
    }

    public class WriteLocked<B extends JData> implements DataAccessor<B> {
        public WriteLocked() {
            _lock.writeLock().lock();
        }

        @Override
        public void close() {
            _lock.writeLock().unlock();
        }

        @Override
        public B getData() {
            return (B) _data;
        }
    }

    public WriteLocked<T> write() {
        return new WriteLocked<>();
    }
}
