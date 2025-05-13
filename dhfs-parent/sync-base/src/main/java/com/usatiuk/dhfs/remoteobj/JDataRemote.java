package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Interface for a remote object. Remote objects are objects that are automatically synchronized between peers,
 * and versioned using a version vector.
 */
public interface JDataRemote extends Serializable {
    /**
     * Returns the key of this remote object.
     *
     * @return the key of this remote object
     */
    JObjectKey key();

    /**
     * Returns the estimated size of this remote object in bytes.
     *
     * @return the estimated size of this remote object in bytes
     */
    default int estimateSize() {
        return 100;
    }

    /**
     * Collect outgoing references to other objects.
     *
     * @return list of outgoing references
     */

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }

    /**
     * Returns the class of DTO of this object that should be used for remote synchronization.
     * It can be the same as the object.
     *
     * @return the class of DTO of this object that should be used for remote synchronization
     */
    default Class<? extends JDataRemoteDto> dtoClass() {
        assert JDataRemoteDto.class.isAssignableFrom(getClass());
        return (Class<? extends JDataRemoteDto>) this.getClass();
    }
}
