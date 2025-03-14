package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;

import java.util.Optional;

public record TransactionObjectNoLock<T extends JData>
        (Optional<JDataVersionedWrapper> data)
        implements TransactionObject<T> {
}
