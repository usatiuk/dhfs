package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionPrivate;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TransactionManagerImpl implements TransactionManager {
    @Inject
    JObjectManager jObjectManager;

    private static final ThreadLocal<TransactionPrivate> _currentTransaction = new ThreadLocal<>();

    @Override
    public void begin() {
        if (_currentTransaction.get() != null) {
            throw new IllegalStateException("Transaction already started");
        }

        Log.trace("Starting transaction");
        var tx = jObjectManager.createTransaction();
        _currentTransaction.set(tx);
    }

    @Override
    public void commit() {
        if (_currentTransaction.get() == null) {
            throw new IllegalStateException("No transaction started");
        }

        Log.trace("Committing transaction");
        try {
            jObjectManager.commit(_currentTransaction.get());
        } catch (Throwable e) {
            Log.warn("Transaction commit failed", e);
            throw e;
        } finally {
            _currentTransaction.remove();
        }
    }

    @Override
    public void rollback() {
        _currentTransaction.remove();
    }

    @Override
    public Transaction current() {
        return _currentTransaction.get();
    }
}

