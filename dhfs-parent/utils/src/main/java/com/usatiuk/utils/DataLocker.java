package com.usatiuk.utils;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.lang.ref.Cleaner;
import java.util.concurrent.ConcurrentHashMap;

public class DataLocker {
    private static final AutoCloseableNoThrow DUMMY_LOCK = () -> {
    };
    private final ConcurrentHashMap<Object, LockTag> _locks = new ConcurrentHashMap<>();

    @Nonnull
    public AutoCloseableNoThrow lock(Object data) {
        while (true) {
            var newTag = new LockTag();
            var oldTag = _locks.putIfAbsent(data, newTag);
            if (oldTag == null) {
                return new Lock(data, newTag);
            }
            try {
                synchronized (oldTag) {
                    while (!oldTag.released) {
                        if (oldTag.owner == Thread.currentThread()) {
                            return DUMMY_LOCK;
                        }
                        oldTag.wait();
//                            tag.wait(4000L);
//                            if (!tag.released) {
//                                System.out.println("Timeout waiting for lock: " + data);
//                                System.exit(1);
//                                throw new InterruptedException();
//                            }
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Nullable
    public AutoCloseableNoThrow tryLock(Object data) {
        while (true) {
            var newTag = new LockTag();
            var oldTag = _locks.putIfAbsent(data, newTag);
            if (oldTag == null) {
                return new Lock(data, newTag);
            }
            synchronized (oldTag) {
                if (!oldTag.released) {
                    if (oldTag.owner == Thread.currentThread()) {
                        return DUMMY_LOCK;
                    }
                    return null;
                }
            }
        }
    }

    private static class LockTag {
        final Thread owner = Thread.currentThread();
        //        final StackTraceElement[] _creationStack = Thread.currentThread().getStackTrace();
        boolean released = false;
    }

    private class Lock implements AutoCloseableNoThrow {
        private static final Cleaner CLEANER = Cleaner.create();
        private final Object _key;
        private final LockTag _tag;

        public Lock(Object key, LockTag tag) {
            _key = key;
            _tag = tag;
//            CLEANER.register(this, () -> {
//                if (!tag.released) {
//                    Log.error("Lock collected without release: " + key);
//                }
//            });
        }

        @Override
        public void close() {
            synchronized (_tag) {
                if (_tag.released)
                    return;
                _tag.released = true;
                // Notify all because when the object is locked again,
                // it's a different lock tag
                _tag.notifyAll();
                _locks.remove(_key, _tag);
            }
        }
    }

}
