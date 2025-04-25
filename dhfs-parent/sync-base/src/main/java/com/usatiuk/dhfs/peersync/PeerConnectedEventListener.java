package com.usatiuk.dhfs.peersync;

public interface PeerConnectedEventListener {
    void handlePeerConnected(PeerId peerId);
}
