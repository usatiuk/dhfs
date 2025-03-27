package com.usatiuk.objects;

public interface JDataVersionedWrapper {
    JData data();

    long version();

    int estimateSize();
}
