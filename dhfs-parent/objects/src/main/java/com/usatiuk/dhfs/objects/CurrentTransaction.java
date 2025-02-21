package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class CurrentTransaction implements Transaction {
    @Inject
    TransactionManager transactionManager;

    @Override
    public long getId() {
        return transactionManager.current().getId();
    }

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

    @Nonnull
    @Override
    public Collection<JObjectKey> findAllObjects() {
        return transactionManager.current().findAllObjects();
    }

    @Override
    public <T extends JData> void put(JData obj) {
        transactionManager.current().put(obj);
    }
}
