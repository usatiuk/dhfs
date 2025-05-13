package com.usatiuk.dhfs.peersync;

/**
 * Listener for peer disconnected events.
 */
public interface PeerDisconnectedEventListener {
    /**
     * Called when a peer is disconnected.
     *
     * @param peerId the ID of the disconnected peer
     */
    void handlePeerDisconnected(PeerId peerId);
}
