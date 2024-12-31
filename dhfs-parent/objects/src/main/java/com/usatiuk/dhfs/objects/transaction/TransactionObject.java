package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JDataVersionedWrapper;
import com.usatiuk.objects.common.runtime.JData;

import java.util.Optional;

public interface TransactionObject<T extends JData> {
    Optional<JDataVersionedWrapper<T>> data();
}
