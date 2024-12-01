package com.usatiuk.dhfs.utils;

import java.util.concurrent.ConcurrentHashMap;

public class DataLocker {
    private static class LockTag {
        boolean released = false;
    }

    private final ConcurrentHashMap<Object, LockTag> _locks = new ConcurrentHashMap<>();

    public class Lock implements AutoCloseable {
        private final Object _key;
        private final LockTag _tag;

        public Lock(Object key, LockTag tag) {
            _key = key;
            _tag = tag;
        }

        @Override
        public void close() {
            synchronized (_tag) {
                _tag.released = true;
                _tag.notifyAll();
                _locks.remove(_key, _tag);
            }
        }
    }

    public Lock lock(Object data) {
        while (true) {
            try {
                var tag = _locks.get(data);
                if (tag != null) {
                    synchronized (tag) {
                        if (!tag.released)
                            tag.wait();
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
