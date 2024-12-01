package com.usatiuk.dhfs.objects;

public interface TxBundle {
    long getId();

    void commit(JData obj);

    void delete(JData obj);
}
