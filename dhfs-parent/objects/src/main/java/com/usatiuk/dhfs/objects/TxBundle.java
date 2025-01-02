package com.usatiuk.dhfs.objects;

public interface TxBundle {
    long getId();

    void commit(JDataVersionedWrapper<?> obj);

    void delete(JObjectKey obj);
}
