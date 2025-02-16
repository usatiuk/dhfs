package com.usatiuk.dhfs.objects.transaction;

import java.util.Collection;

public interface TransactionHandlePrivate extends TransactionHandle {
    Collection<Runnable> getOnFlush();
}
