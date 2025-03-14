package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.iterators.CloseableKvIterator;
import com.usatiuk.dhfs.objects.iterators.IteratorStart;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Optional;

@ApplicationScoped
public class CurrentTransaction implements Transaction {
    @Inject
    TransactionManager transactionManager;

    @Override
    public void onCommit(Runnable runnable) {
        transactionManager.current().onCommit(runnable);
    }

    @Override
    public void onFlush(Runnable runnable) {
        transactionManager.current().onFlush(runnable);
    }

    @Override
    public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return transactionManager.current().get(type, key, strategy);
    }

    @Override
    public void delete(JObjectKey key) {
        transactionManager.current().delete(key);
    }

    @Override
    public CloseableKvIterator<JObjectKey, JData> getIterator(IteratorStart start, JObjectKey key) {
        return transactionManager.current().getIterator(start, key);
    }

    @Override
    public <T extends JData> void put(JData obj) {
        transactionManager.current().put(obj);
    }
}
