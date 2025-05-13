package com.usatiuk.dhfs.invalidation;

import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents a unit of information to be sent to another peer.
 * The operations are extracted from objects in the key-value storage, and then sent to peers.
 */
public interface Op extends Serializable {
    /**
     * Returns the keys of the objects that are referenced in this op.
     * These objects should be marked as "escaped" in the local storage for the purposed of garbage collection.
     *
     * @return the keys of the objects that are referenced in this operation
     */
    Collection<JObjectKey> getEscapedRefs();
}
