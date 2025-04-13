package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;

public interface PeerConnectedEventListener {
    void handlePeerConnected(PeerId peerId);
}
