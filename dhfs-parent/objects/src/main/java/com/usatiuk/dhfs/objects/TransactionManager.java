package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.utils.VoidFn;

import java.util.function.Supplier;

public interface TransactionManager {
    void begin();

    void commit();

    void rollback();

    default <T> T run(Supplier<T> supplier) {
        if (current() != null) {
            return supplier.get();
        }

        begin();
        try {
            var ret = supplier.get();
            commit();
            return ret;
        } catch (TxCommitException txCommitException) {
            return run(supplier);
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }

    default void run(VoidFn fn) {
        if (current() != null) {
            fn.apply();
            return;
        }

        begin();
        try {
            fn.apply();
            commit();
        } catch (TxCommitException txCommitException) {
            run(fn);
            return;
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }

    default void executeTx(VoidFn fn) {
        run(fn);
    }

    default <T> T executeTx(Supplier<T> supplier) {
        return run(supplier);
    }

    Transaction current();
}
