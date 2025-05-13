package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.PMap;

import javax.annotation.Nullable;

/**
 * Interface for handling remote updates of objects.
 *
 * @param <T> the type of the remote object
 * @param <D> the type of the remote object DTO
 */
public interface ObjSyncHandler<T extends JDataRemote, D extends JDataRemoteDto> {
    /**
     * Handles a remote update of an object.
     *
     * @param from              the ID of the peer that sent the update
     * @param key               the key of the object
     * @param receivedChangelog the changelog received from the peer
     * @param receivedData      the data received from the peer
     */
    void handleRemoteUpdate(PeerId from, JObjectKey key,
                            PMap<PeerId, Long> receivedChangelog,
                            @Nullable D receivedData);
}
