package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JDataVersionedWrapper;

import java.util.Optional;

public interface TransactionObject<T extends JData> {
    Optional<JDataVersionedWrapper> data();
}
