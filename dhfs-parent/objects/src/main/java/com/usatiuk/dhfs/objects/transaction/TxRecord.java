package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.alloc.runtime.ChangeTrackingJData;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public class TxRecord {
    public interface TxObjectRecord<T> {
        T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy);
    }

    public record TxObjectRecordMissing<T extends JData>(JObjectKey key) implements TxObjectRecord<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            return null;
        }
    }

    public interface TxObjectRecordWrite<T extends JData> extends TxObjectRecord<T> {
        TransactionObject<T> original();

        ChangeTrackingJData<T> copy();
    }

    public record TxObjectRecordNew<T extends JData>(T created) implements TxObjectRecord<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return created;
            return null;
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
