package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.objects.snapshot.SnapshotManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;
import java.util.*;

@ApplicationScoped
public class TransactionFactoryImpl implements TransactionFactory {
    @Inject
    SnapshotManager snapshotManager;
    @Inject
    ReadTrackingObjectSourceFactory readTrackingObjectSourceFactory;

    @Override
    public TransactionPrivate createTransaction() {
        return new TransactionImpl();
    }

    private class TransactionImpl implements TransactionPrivate {
        private final ReadTrackingTransactionObjectSource _source;

        private final NavigableMap<JObjectKey, TxRecord.TxObjectRecord<?>> _writes = new TreeMap<>();

        private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _newWrites = new HashMap<>();
        private final List<Runnable> _onCommit = new ArrayList<>();
        private final List<Runnable> _onFlush = new ArrayList<>();
        private final SnapshotManager.Snapshot _snapshot;

        private TransactionImpl() {
            _snapshot = snapshotManager.createSnapshot();
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
        public CloseableKvIterator<JObjectKey, JData> getIterator(IteratorStart start, JObjectKey key) {
            Log.tracev("Getting tx iterator with start={0}, key={1}", start, key);
            return new TombstoneMergingKvIterator<>("tx", start, key,
                    (tS, tK) -> new MappingKvIterator<>(new NavigableMapKvIterator<>(_writes, tS, tK), t -> switch (t) {
                        case TxRecord.TxObjectRecordWrite<?> write -> new Data<>(write.data());
                        case TxRecord.TxObjectRecordDeleted deleted -> new Tombstone<>();
                        case null, default -> null;
                    }),
                    (tS, tK) -> new MappingKvIterator<>(_source.getIterator(tS, tK), Data::new));
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
