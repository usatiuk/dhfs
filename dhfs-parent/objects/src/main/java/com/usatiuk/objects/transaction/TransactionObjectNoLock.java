package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;

import java.util.Optional;

public record TransactionObjectNoLock<T extends JData>
        (Optional<JDataVersionedWrapper> data)
        implements TransactionObject<T> {
}
