package com.usatiuk.dhfs.remoteobj;

import java.io.Serializable;

/**
 * Marker interface for a DTO class to be used when synchronizing some remote object.
 */
public interface JDataRemoteDto extends Serializable {
    /**
     * Returns the class of the remote object that this DTO represents.
     *
     * @return the class of the remote object that this DTO represents
     */
    default Class<? extends JDataRemote> objClass() {
        assert JDataRemote.class.isAssignableFrom(getClass());
        return (Class<? extends JDataRemote>) this.getClass();
    }
}
