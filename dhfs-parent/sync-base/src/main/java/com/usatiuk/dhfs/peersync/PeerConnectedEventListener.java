package com.usatiuk.dhfs.peersync;

/**
 * Listener for peer connected events.
 */
public interface PeerConnectedEventListener {
    /**
     * Called when a peer is connected.
     *
     * @param peerId the ID of the connected peer
     */
    void handlePeerConnected(PeerId peerId);
}
