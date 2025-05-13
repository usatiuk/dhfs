package com.usatiuk.kleppmanntree;

import java.util.Collection;

/**
 * Interface providing access to a list of known peers.
 * @param <PeerIdT> the type of the peer ID
 */
public interface PeerInterface<PeerIdT extends Comparable<PeerIdT>> {
    /**
     * Returns the ID of the current peer.
     *
     * @return the ID of the current peer
     */
    PeerIdT getSelfId();

    /**
     * Returns a collection of all known peers.
     *
     * @return a collection of all known peers
     */
    Collection<PeerIdT> getAllPeers();
}
