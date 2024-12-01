package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionPrivate;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TransactionManagerImpl implements TransactionManager {
    @Inject
    JObjectManager objectManager;

    private static final ThreadLocal<TransactionPrivate> _currentTransaction = new ThreadLocal<>();

    @Override
    public void begin() {
        if (_currentTransaction.get() != null) {
            throw new IllegalStateException("Transaction already started");
        }

        var tx = objectManager.createTransaction();
        _currentTransaction.set(tx);
    }

    @Override
    public void commit() {
        if(_currentTransaction.get() == null) {
            throw new IllegalStateException("No transaction started");
        }

        jobjectManager.commit(_currentTransaction.get());
    }

    @Override
    public void rollback() {

    }

    @Override
    public Transaction current() {
        return _currentTransaction.get();
    }

}

