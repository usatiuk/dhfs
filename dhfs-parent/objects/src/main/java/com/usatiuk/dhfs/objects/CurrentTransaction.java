package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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
    public <T extends JData> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return transactionManager.current().get(type, key, strategy);
    }

    @Override
    public void delete(JObjectKey key) {
        transactionManager.current().delete(key);
    }

    @Override
    public <T extends JData> void put(JData obj) {
        transactionManager.current().put(obj);
    }
}
