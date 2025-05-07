package com.usatiuk.objects.transaction;

import io.quarkus.logging.Log;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.Stack;

@Singleton
public class TransactionManagerImpl implements TransactionManager {
    private static final ThreadLocal<Stack<TransactionImpl>> _currentTransaction = ThreadLocal.withInitial(Stack::new);
    @Inject
    TransactionService transactionService;

    @Override
    public void begin() {
        Log.trace("Starting transaction");
        var tx = transactionService.createTransaction();
        _currentTransaction.get().push(tx);
    }

    @Override
    public TransactionHandle commit() {
        var stack = _currentTransaction.get();
        if (stack.empty()) {
            throw new IllegalStateException("No transaction started");
        }
        var peeked = stack.peek();

        Log.trace("Committing transaction");

        Pair<Collection<Runnable>, TransactionHandle> ret;
        try {
            ret = transactionService.commit(peeked);
        } catch (Throwable e) {
            Log.trace("Transaction commit failed", e);
            throw e;
        } finally {
            peeked.close();
            stack.pop();
            if (stack.empty())
                _currentTransaction.remove();
        }

        for (var r : ret.getLeft()) {
            try {
                r.run();
            } catch (Throwable e) {
                Log.error("Transaction commit hook error: ", e);
            }
        }
        return ret.getRight();
    }

    @Override
    public void rollback() {
        var stack = _currentTransaction.get();
        if (stack.empty()) {
            throw new IllegalStateException("No transaction started");
        }
        var peeked = stack.peek();

        try {
            transactionService.rollback(peeked);
        } catch (Throwable e) {
            Log.error("Transaction rollback failed", e);
            throw e;
        } finally {
            peeked.close();
            stack.pop();
            if (stack.empty())
                _currentTransaction.remove();
        }
    }

    @Override
    @Nullable
    public Transaction current() {
        var stack = _currentTransaction.get();
        if (stack.empty()) {
            return null;
        }
        return stack.peek();
    }
}

