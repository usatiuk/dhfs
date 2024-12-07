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
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }


    Transaction current();
}
