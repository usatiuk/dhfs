package com.usatiuk.dhfs.syncmap;

import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;

/**
 * Interface for mapping between a remote object and its DTO representation.
 *
 * @param <F> the type of the remote object
 * @param <D> the type of the DTO
 */
public interface DtoMapper<F extends JDataRemote, D extends JDataRemoteDto> {
    /**
     * Converts a remote object to its DTO representation.
     *
     * @param obj the remote object to convert
     * @return the DTO representation of the remote object
     */
    D toDto(F obj);

    /**
     * Converts a DTO to its corresponding remote object.
     *
     * @param dto the DTO to convert
     * @return the remote object representation of the DTO
     */
    F fromDto(D dto);
}
