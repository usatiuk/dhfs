package com.usatiuk.dhfs.peersync;

public interface PeerDisconnectedEventListener {
    void handlePeerDisconnected(PeerId peerId);
}
