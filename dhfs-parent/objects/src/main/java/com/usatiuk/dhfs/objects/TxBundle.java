package com.usatiuk.dhfs.objects;

import com.usatiuk.objects.common.runtime.JData;

public interface TxBundle {
    long getId();

    void commit(JData obj);

    void delete(JData obj);
}
