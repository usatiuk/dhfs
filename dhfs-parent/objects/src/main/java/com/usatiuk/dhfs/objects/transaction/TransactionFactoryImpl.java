package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.SnapshotManager;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.*;

@ApplicationScoped
public class TransactionFactoryImpl implements TransactionFactory {
    @Inject
    SnapshotManager snapshotManager;
    @Inject
    ReadTrackingObjectSourceFactory readTrackingObjectSourceFactory;

    @Override
    public TransactionPrivate createTransaction(long snapshotId) {
        return new TransactionImpl(snapshotId);
    }

    private class TransactionImpl implements TransactionPrivate {
        private final ReadTrackingTransactionObjectSource _source;
        private final Map<JObjectKey, TxRecord.TxObjectRecord<?>> _writes = new HashMap<>();
        private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _newWrites = new HashMap<>();
        private final List<Runnable> _onCommit = new ArrayList<>();
        private final List<Runnable> _onFlush = new ArrayList<>();
        private final SnapshotManager.Snapshot _snapshot;

        private TransactionImpl(long snapshotId) {
            _snapshot = snapshotManager.createSnapshot(snapshotId);
            _source = readTrackingObjectSourceFactory.create(_snapshot);
        }

        @Override
        public void onCommit(Runnable runnable) {
            _onCommit.add(runnable);
        }

        @Override
        public void onFlush(Runnable runnable) {
            _onFlush.add(runnable);
        }

        @Override
        public Collection<Runnable> getOnCommit() {
            return Collections.unmodifiableCollection(_onCommit);
        }

        @Override
        public SnapshotManager.Snapshot snapshot() {
            return _snapshot;
        }

        @Override
        public Collection<Runnable> getOnFlush() {
            return Collections.unmodifiableCollection(_onFlush);
        }

        @Override
        public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
            switch (_writes.get(key)) {
                case TxRecord.TxObjectRecordWrite<?> write -> {
                    return Optional.of(type.cast(write.data()));
                }
                case TxRecord.TxObjectRecordDeleted deleted -> {
                    return Optional.empty();
                }
                case null, default -> {
                }
            }

            return switch (strategy) {
                case OPTIMISTIC -> _source.get(type, key);
                case WRITE -> _source.getWriteLocked(type, key);
            };
        }

        @Override
        public void delete(JObjectKey key) {
            var got = _writes.get(key);
            if (got != null) {
                if (got instanceof TxRecord.TxObjectRecordDeleted) {
                    return;
                }
            }

            _writes.put(key, new TxRecord.TxObjectRecordDeleted(key));
            _newWrites.put(key, new TxRecord.TxObjectRecordDeleted(key));
        }

        @Nonnull
        @Override
        public Collection<JObjectKey> findAllObjects() {
//            return store.findAllObjects();
            return List.of();
        }

        @Override
        public Iterator<Pair<JObjectKey, JData>> getIterator(IteratorStart start, JObjectKey key) {
            return _source.getIterator(start, key);
        }

        @Override
        public void put(JData obj) {
            _writes.put(obj.key(), new TxRecord.TxObjectRecordWrite<>(obj));
            _newWrites.put(obj.key(), new TxRecord.TxObjectRecordWrite<>(obj));
        }

        @Override
        public Collection<TxRecord.TxObjectRecord<?>> drainNewWrites() {
            var ret = _newWrites;
            _newWrites = new HashMap<>();
            return ret.values();
        }

        @Override
        public Map<JObjectKey, TransactionObject<?>> reads() {
            return _source.getRead();
        }

        @Override
        public ReadTrackingTransactionObjectSource readSource() {
            return _source;
        }

        @Override
        public void close() {
            _source.close();
            _snapshot.close();
        }
    }
}
