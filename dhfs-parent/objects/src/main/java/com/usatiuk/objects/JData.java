package com.usatiuk.objects;

import java.io.Serializable;

/**
 * JData is a marker interface for all objects that can be stored in the object store.
 */
public interface JData extends Serializable {
    /**
     * Returns the key of the object.
     * @return the key of the object
     */
    JObjectKey key();

    /**
     * Returns the estimated size of the object in bytes.
     * @return the estimated size of the object in bytes
     */
    default int estimateSize() {
        return 100;
    }
}
