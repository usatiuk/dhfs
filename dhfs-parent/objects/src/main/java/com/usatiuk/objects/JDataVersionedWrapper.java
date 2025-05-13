package com.usatiuk.objects;

import com.usatiuk.objects.iterators.Data;

/**
 * JDataVersionedWrapper is a wrapper for JData that contains its version number
 * (the id of the transaction that had changed it last)
 */
public sealed interface JDataVersionedWrapper extends Data<JDataVersionedWrapper> permits JDataVersionedWrapperLazy, JDataVersionedWrapperImpl {
    @Override
    default JDataVersionedWrapper value() {
        return this;
    }

    /**
     * Returns the wrapped object.
     *
     * @return the wrapped object
     */
    JData data();

    /**
     * Returns the version number of the object.
     *
     * @return the version number of the object
     */
    long version();

    /**
     * Returns the estimated size of the object in bytes.
     *
     * @return the estimated size of the object in bytes
     */
    int estimateSize();
}
