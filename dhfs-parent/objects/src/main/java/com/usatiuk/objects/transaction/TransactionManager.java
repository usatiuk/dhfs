package com.usatiuk.objects.transaction;

import com.usatiuk.utils.VoidFn;
import io.quarkus.logging.Log;

import java.util.function.Supplier;

public interface TransactionManager {
    void begin();

    TransactionHandle commit();

    void rollback();

    default <T> T runTries(Supplier<T> supplier, int tries, boolean nest) {
        if (!nest && current() != null) {
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

    default TransactionHandle runTries(VoidFn fn, int tries, boolean nest) {
        if (!nest && current() != null) {
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

    default <T> T runTries(Supplier<T> supplier, int tries) {
        return runTries(supplier, tries, false);
    }

    default TransactionHandle runTries(VoidFn fn, int tries) {
        return runTries(fn, tries, false);
    }

    default TransactionHandle run(VoidFn fn, boolean nest) {
        return runTries(fn, 10, nest);
    }

    default <T> T run(Supplier<T> supplier, boolean nest) {
        return runTries(supplier, 10, nest);
    }

    default TransactionHandle run(VoidFn fn) {
        return run(fn, false);
    }

    default <T> T run(Supplier<T> supplier) {
        return run(supplier, false);
    }

    default void executeTx(VoidFn fn) {
        run(fn, false);
    }

    default <T> T executeTx(Supplier<T> supplier) {
        return run(supplier, false);
    }

    Transaction current();
}
