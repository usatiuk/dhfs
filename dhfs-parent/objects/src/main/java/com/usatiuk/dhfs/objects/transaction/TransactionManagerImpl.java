package com.usatiuk.dhfs.objects.transaction;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TransactionManagerImpl implements TransactionManager {
    private static final ThreadLocal<TransactionPrivate> _currentTransaction = new ThreadLocal<>();
    @Inject
    JObjectManager jObjectManager;

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
    public TransactionHandle commit() {
        if (_currentTransaction.get() == null) {
            throw new IllegalStateException("No transaction started");
        }

        Log.trace("Committing transaction");
        try {
            return jObjectManager.commit(_currentTransaction.get());
        } catch (Throwable e) {
            Log.trace("Transaction commit failed", e);
            throw e;
        } finally {
            _currentTransaction.get().close();
            _currentTransaction.remove();
        }
    }

    @Override
    public void rollback() {
        if (_currentTransaction.get() == null) {
            throw new IllegalStateException("No transaction started");
        }

        try {
            jObjectManager.rollback(_currentTransaction.get());
        } catch (Throwable e) {
            Log.error("Transaction rollback failed", e);
            throw e;
        } finally {
            _currentTransaction.get().close();
            _currentTransaction.remove();
        }
    }

    @Override
    public Transaction current() {
        return _currentTransaction.get();
    }
}

