package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
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
    public <T extends JData> Optional<T> getObject(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return transactionManager.current().getObject(type, key, strategy);
    }

    @Override
    public void deleteObject(JObjectKey key) {
        transactionManager.current().deleteObject(key);
    }

    @Override
    public <T extends JData> void putObject(JData obj) {
        transactionManager.current().putObject(obj);
    }
}
