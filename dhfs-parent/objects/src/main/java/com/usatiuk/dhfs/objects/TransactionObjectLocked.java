package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.TransactionObject;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

import java.util.Optional;

public record TransactionObjectLocked<T extends JData>
        (Optional<JDataVersionedWrapper> data, AutoCloseableNoThrow lock)
        implements TransactionObject<T> {
}
