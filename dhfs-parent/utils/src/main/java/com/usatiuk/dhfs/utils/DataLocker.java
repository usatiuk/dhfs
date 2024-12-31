package com.usatiuk.dhfs.utils;

import io.quarkus.logging.Log;

import java.lang.ref.Cleaner;
import java.util.concurrent.ConcurrentHashMap;

public class DataLocker {
    private static class LockTag {
        boolean released = false;
        final Thread owner = Thread.currentThread();
    }

    private final ConcurrentHashMap<Object, LockTag> _locks = new ConcurrentHashMap<>();

    private class Lock implements AutoCloseableNoThrow {
        private final Object _key;
        private final LockTag _tag;
        private static final Cleaner CLEANER = Cleaner.create();

        public Lock(Object key, LockTag tag) {
            _key = key;
            _tag = tag;
            CLEANER.register(this, () -> {
                if (!tag.released) {
                    Log.error("Lock collected without release: " + key);
                }
            });
        }

        @Override
        public void close() {
            synchronized (_tag) {
                _tag.released = true;
                _tag.notify();
                _locks.remove(_key, _tag);
            }
        }
    }

    private static final AutoCloseableNoThrow DUMMY_LOCK = () -> {
    };

    public AutoCloseableNoThrow lock(Object data) {
        while (true) {
            try {
                var tag = _locks.get(data);
                if (tag != null) {
                    synchronized (tag) {
                        if (!tag.released) {
                            if (tag.owner == Thread.currentThread()) {
                                return DUMMY_LOCK;
                            }
                            tag.wait();
                        }
                        continue;
                    }
                }
            } catch (InterruptedException ignored) {
            }

            var newTag = new LockTag();
            var oldTag = _locks.putIfAbsent(data, newTag);
            if (oldTag == null) {
                return new Lock(data, newTag);
            }
        }
    }

}
