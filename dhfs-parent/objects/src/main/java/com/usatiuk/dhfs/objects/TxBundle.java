package com.usatiuk.dhfs.objects;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public interface TxBundle {
    long getId();

    void commit(JDataVersionedWrapper<?> obj);

    void delete(JObjectKey obj);
}
