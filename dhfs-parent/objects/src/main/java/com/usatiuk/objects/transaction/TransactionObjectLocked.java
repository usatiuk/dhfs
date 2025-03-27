package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

import java.util.Optional;

public record TransactionObjectLocked<T extends JData>
        (Optional<JDataVersionedWrapper> data, AutoCloseableNoThrow lock)
        implements TransactionObject<T> {
}
