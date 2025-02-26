package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.snapshot.SnapshotManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class ReadTrackingObjectSourceFactory {
    @Inject
    LockManager lockManager;

    public ReadTrackingTransactionObjectSource create(SnapshotManager.Snapshot snapshot) {
        return new ReadTrackingObjectSourceImpl(snapshot);
    }

    public class ReadTrackingObjectSourceImpl implements ReadTrackingTransactionObjectSource {
        private final SnapshotManager.Snapshot _snapshot;

        private final Map<JObjectKey, TransactionObject<?>> _readSet = new HashMap<>();

        public ReadTrackingObjectSourceImpl(SnapshotManager.Snapshot snapshot) {
            _snapshot = snapshot;
        }

        public Map<JObjectKey, TransactionObject<?>> getRead() {
            return Collections.unmodifiableMap(_readSet);
        }

        @Override
        public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key) {
            var got = _readSet.get(key);

            if (got == null) {
                var read = _snapshot.readObject(key);
                _readSet.put(key, new TransactionObjectNoLock<>(read));
                return read.map(JDataVersionedWrapper::data).map(type::cast);
            }

            return got.data().map(JDataVersionedWrapper::data).map(type::cast);
        }

        @Override
        public <T extends JData> Optional<T> getWriteLocked(Class<T> type, JObjectKey key) {
            var got = _readSet.get(key);

            if (got == null) {
                var lock = lockManager.lockObject(key);
                try {
                    var read = _snapshot.readObject(key);
                    _readSet.put(key, new TransactionObjectLocked<>(read, lock));
                    return read.map(JDataVersionedWrapper::data).map(type::cast);
                } catch (Exception e) {
                    lock.close();
                    throw e;
                }
            }

            return got.data().map(JDataVersionedWrapper::data).map(type::cast);
        }

        @Override
        public void close() {
//            for (var it : _iterators) {
//                it.close();
//            }
        }

        private class ReadTrackingIterator implements CloseableKvIterator<JObjectKey, JData> {
            private final CloseableKvIterator<JObjectKey, JDataVersionedWrapper> _backing;

            public ReadTrackingIterator(IteratorStart start, JObjectKey key) {
                _backing = _snapshot.getIterator(start, key);
            }

            @Override
            public JObjectKey peekNextKey() {
                return _backing.peekNextKey();
            }

            @Override
            public void skip() {
                _backing.skip();
            }

            @Override
            public void close() {
                _backing.close();
            }

            @Override
            public boolean hasNext() {
                return _backing.hasNext();
            }

            @Override
            public Pair<JObjectKey, JData> next() {
                var got = _backing.next();
                _readSet.putIfAbsent(got.getKey(), new TransactionObjectNoLock<>(Optional.of(got.getValue())));
                return Pair.of(got.getKey(), got.getValue().data());
            }
        }

        @Override
        public CloseableKvIterator<JObjectKey, JData> getIterator(IteratorStart start, JObjectKey key) {
            return new ReadTrackingIterator(start, key);
        }
    }
}