package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.JData;
import com.usatiuk.objects.common.JObjectKey;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;

public class TxRecord {
    public interface TxObjectRecord<T> {
        T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy);
    }

    public interface TxObjectRecordWrite<T extends JData> extends TxObjectRecord<T> {
        ObjectAllocator.ChangeTrackingJData<T> copy();
    }

    public record TxObjectRecordRead<T extends JData>(TransactionObjectSource.TransactionObject<T> original,
                                                      T copy)
            implements TxObjectRecord<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.READ_ONLY)
                return copy;
            return null;
        }
    }

    public record TxObjectRecordNew<T extends JData>(T created)
            implements TxObjectRecordWrite<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return created;
            return null;
        }

        @Override
        public ObjectAllocator.ChangeTrackingJData<T> copy() {
            return new ObjectAllocator.ChangeTrackingJData<T>() {
                @Override
                public T wrapped() {
                    return created;
                }

                @Override
                public boolean isModified() {
                    return true;
                }
            };
        }
    }

    public record TxObjectRecordCopyLock<T extends JData>(TransactionObjectSource.TransactionObject<T> original,
                                                                                     ObjectAllocator.ChangeTrackingJData<T> copy)
            implements TxObjectRecordWrite<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return copy.wrapped();
            return null;
        }
    }

    public record TxObjectRecordCopyNoLock<T extends JData>(T original,
                                                            ObjectAllocator.ChangeTrackingJData<T> copy)
            implements TxObjectRecordWrite<T> {
        @Override
        public T getIfStrategyCompatible(JObjectKey key, LockingStrategy strategy) {
            if (strategy == LockingStrategy.WRITE || strategy == LockingStrategy.OPTIMISTIC)
                return copy.wrapped();
            return null;
        }
    }
}
