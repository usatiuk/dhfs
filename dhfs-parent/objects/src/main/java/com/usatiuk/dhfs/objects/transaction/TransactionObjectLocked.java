package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

import java.util.Optional;

public record TransactionObjectLocked<T extends JData>
        (Optional<JDataVersionedWrapper> data, AutoCloseableNoThrow lock)
        implements TransactionObject<T> {
}
