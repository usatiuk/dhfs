package com.usatiuk.objects;

import com.usatiuk.objects.iterators.Data;

public sealed interface JDataVersionedWrapper extends Data<JDataVersionedWrapper> permits JDataVersionedWrapperLazy, JDataVersionedWrapperImpl {
    @Override
    default JDataVersionedWrapper value() {
        return this;
    }

    JData data();

    long version();

    int estimateSize();
}
