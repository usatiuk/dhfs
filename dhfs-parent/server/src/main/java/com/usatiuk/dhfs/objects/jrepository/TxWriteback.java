package com.usatiuk.dhfs.objects.jrepository;

public interface TxWriteback {
    TxBundle createBundle();

    void commitBundle(TxBundle bundle);

    void fence(long txId);
}
