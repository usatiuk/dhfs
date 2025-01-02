package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.utils.VoidFn;
import io.quarkus.logging.Log;

import java.util.function.Supplier;

public interface TransactionManager {
    void begin();

    void commit();

    void rollback();

    default <T> T runTries(Supplier<T> supplier, int tries) {
        if (current() != null) {
            return supplier.get();
        }

        begin();
        try {
            var ret = supplier.get();
            commit();
            return ret;
        } catch (TxCommitException txCommitException) {
            if (tries == 0) {
                Log.error("Transaction commit failed", txCommitException);
                throw txCommitException;
            }
            return runTries(supplier, tries - 1);
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }

    default void runTries(VoidFn fn, int tries) {
        if (current() != null) {
            fn.apply();
            return;
        }

        begin();
        try {
            fn.apply();
            commit();
        } catch (TxCommitException txCommitException) {
            if (tries == 0) {
                Log.error("Transaction commit failed", txCommitException);
                throw txCommitException;
            }
            runTries(fn, tries - 1);
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }

    default void run(VoidFn fn) {
        runTries(fn, 10);
    }

    default <T> T run(Supplier<T> supplier) {
        return runTries(supplier, 10);
    }

    default void executeTx(VoidFn fn) {
        run(fn);
    }

    default <T> T executeTx(Supplier<T> supplier) {
        return run(supplier);
    }

    Transaction current();
}
