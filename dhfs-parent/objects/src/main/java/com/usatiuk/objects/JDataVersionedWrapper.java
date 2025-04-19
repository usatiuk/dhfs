package com.usatiuk.objects;

public sealed interface JDataVersionedWrapper permits JDataVersionedWrapperLazy, JDataVersionedWrapperImpl {
    JData data();

    long version();

    int estimateSize();
}
