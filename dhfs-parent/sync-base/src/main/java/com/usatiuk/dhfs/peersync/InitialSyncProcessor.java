package com.usatiuk.dhfs.peersync;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

/**
 * Allows to specify custom processing of initial synchronization/crash recovery for a specific object type.
 *
 * @param <T> the type of the object
 */
public interface InitialSyncProcessor<T extends JData> {
    /**
     * Called when  the peer is connected for the first time (or needs to be re-synced).
     *
     * @param from the peer that initiated the sync
     * @param key  the key of the object to be synchronized
     */
    void prepareForInitialSync(PeerId from, JObjectKey key);

    /**
     * Called when the system had crashed (and the object needs to probably be re-synced).
     *
     * @param key the key of the object to be handled
     */
    void handleCrash(JObjectKey key);
}
