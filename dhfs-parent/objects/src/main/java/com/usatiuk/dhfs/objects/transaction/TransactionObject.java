package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.dhfs.objects.JData;

import java.util.Optional;

public interface TransactionObject<T extends JData> {
    Optional<JDataVersionedWrapper<T>> data();
}
