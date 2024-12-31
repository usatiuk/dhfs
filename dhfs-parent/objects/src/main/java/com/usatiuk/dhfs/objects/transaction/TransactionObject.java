package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;

import java.util.Optional;

public interface TransactionObject<T extends JData> {
    Optional<T> data();
}
