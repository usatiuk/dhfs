package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.TransactionObject;

import java.util.Optional;

public record TransactionObjectNoLock<T extends JData>
        (Optional<JDataVersionedWrapper> data)
        implements TransactionObject<T> {
}
