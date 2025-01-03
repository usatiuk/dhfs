package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JObjectKey;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class TransactionFactoryImpl implements TransactionFactory {
    @Override
    public TransactionPrivate createTransaction(long id, TransactionObjectSource source) {
        return new TransactionImpl(id, source);
    }

    private class TransactionImpl implements TransactionPrivate {
        private final long _id;
        private final ReadTrackingObjectSource _source;
        private final Map<JObjectKey, TxRecord.TxObjectRecord<?>> _writes = new HashMap<>();
        private Map<JObjectKey, TxRecord.TxObjectRecord<?>> _newWrites = new HashMap<>();
        private TransactionImpl(long id, TransactionObjectSource source) {
            _id = id;
            _source = new ReadTrackingObjectSource(source);
        }

        public long getId() {
            return _id;
        }

        @Override
        public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
            switch (_writes.get(key)) {
                case TxRecord.TxObjectRecordWrite<?> write -> {
                    if (type.isInstance(write.data())) {
                        return Optional.of((T) write.data());
                    } else {
                        throw new IllegalStateException("Type mismatch for " + key + ": expected " + type + ", got " + write.data().getClass());
                    }
                }
                case TxRecord.TxObjectRecordDeleted deleted -> {
                    return Optional.empty();
                }
                case null, default -> {
                }
            }

            return switch (strategy) {
                case OPTIMISTIC -> _source.get(type, key).data().map(JDataVersionedWrapper::data);
                case WRITE -> _source.getWriteLocked(type, key).data().map(JDataVersionedWrapper::data);
            };
        }

        @Override
        public void delete(JObjectKey key) {
//            get(JData.class, key, LockingStrategy.OPTIMISTIC);

            // FIXME
            var got = _writes.get(key);
            if (got != null) {
                switch (got) {
                    case TxRecord.TxObjectRecordDeleted deleted -> {
                        return;
                    }
                    default -> {
                    }
                }
            }
//
//            var read = _source.get(JData.class, key).orElse(null);
//            if (read == null) {
//                return;
//            }
            _writes.put(key, new TxRecord.TxObjectRecordDeleted(key)); // FIXME:
            _newWrites.put(key, new TxRecord.TxObjectRecordDeleted(key));
        }

        @Override
        public void put(JData obj) {
//            get(JData.class, obj.getKey(), LockingStrategy.OPTIMISTIC);

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
    }

}
