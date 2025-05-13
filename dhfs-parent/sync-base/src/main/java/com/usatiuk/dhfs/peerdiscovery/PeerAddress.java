package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PeerId;

import java.io.Serializable;

/**
 * Peer address interface, can be used to represent different types of peer addresses, not only IP.
 */
public interface PeerAddress extends Serializable {
    /**
     * Returns the peer ID associated with this address.
     *
     * @return the peer ID
     */
    PeerId peer();

    /**
     * Returns the type of this peer address (LAN/WAN/etc)
     *
     * @return the type of the peer address
     */
    PeerAddressType type();
}
