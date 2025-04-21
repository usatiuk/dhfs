package com.usatiuk.objects.transaction;

import com.usatiuk.dhfs.utils.VoidFn;
import io.quarkus.logging.Log;

import java.util.function.Supplier;

public interface TransactionManager {
    void begin();

    TransactionHandle commit();

    void rollback();

    default <T> T runTries(Supplier<T> supplier, int tries) {
        if (current() != null) {
            return supplier.get();
        }

        while (true) {
            begin();
            boolean commit = false;
            try {
                var ret = supplier.get();
                commit = true;
                commit();
                return ret;
            } catch (TxCommitException txCommitException) {
                if (!commit)
                    rollback();
                if (tries == 0) {
                    Log.error("Transaction commit failed", txCommitException);
                    throw txCommitException;
                }
                tries--;
            } catch (Throwable e) {
                if (!commit)
                    rollback();
                throw e;
            }
        }
    }

    default TransactionHandle runTries(VoidFn fn, int tries) {
        if (current() != null) {
            fn.apply();
            return new TransactionHandle() {
                @Override
                public void onFlush(Runnable runnable) {
                    current().onCommit(runnable);
                }
            };
        }

        while (true) {
            begin();
            boolean commit = false;
            try {
                fn.apply();
                commit = true;
                var ret = commit();
                return ret;
            } catch (TxCommitException txCommitException) {
                if (!commit)
                    rollback();
                if (tries == 0) {
                    Log.error("Transaction commit failed", txCommitException);
                    throw txCommitException;
                }
                tries--;
            } catch (Throwable e) {
                if (!commit)
                    rollback();
                throw e;
            }
        }

    }

    default TransactionHandle run(VoidFn fn) {
        return runTries(fn, 10);
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
