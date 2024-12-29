package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.alloc.runtime.ChangeTrackingJData;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public class TxRecord {
    public interface TxObjectRecord<T> {
        T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy);

        JObjectKey getKey();
    }

    public interface TxObjectRecordWrite<T extends JData> extends TxObjectRecord<T> {
        TransactionObject<T> original();

        ChangeTrackingJData<T> copy();

        default JObjectKey getKey() {
            return original().data().getKey();
        }
    }

    public record TxObjectRecordNew<T extends JData>(T created) implements TxObjectRecord<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return created;
            return null;
        }

        @Override
        public JObjectKey getKey() {
            return created.getKey();
        }
    }

    public record TxObjectRecordDeleted<T extends JData>(TransactionObject<T> original,
                                                         T current) implements TxObjectRecord<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            return null;
        }

        @Override
        public JObjectKey getKey() {
            return original.data().getKey();
        }

        public TxObjectRecordDeleted(TxObjectRecordWrite<T> original) {
            this(original.original(), original.copy().wrapped());
        }

        public TxObjectRecordDeleted(TransactionObject<T> original) {
            this(original, original.data());
        }
    }

    public record TxObjectRecordCopyLock<T extends JData>(TransactionObject<T> original,
                                                          ChangeTrackingJData<T> copy)
            implements TxObjectRecordWrite<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return copy.wrapped();
            return null;
        }
    }

    public record TxObjectRecordOptimistic<T extends JData>(TransactionObject<T> original,
                                                            ChangeTrackingJData<T> copy)
            implements TxObjectRecordWrite<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return copy.wrapped();
            return null;
        }
    }
}
