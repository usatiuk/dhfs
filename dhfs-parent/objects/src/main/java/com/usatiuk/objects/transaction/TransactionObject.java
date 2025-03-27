package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JDataVersionedWrapper;

import java.util.Optional;

public interface TransactionObject<T extends JData> {
    Optional<JDataVersionedWrapper> data();
}
