package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;

public interface PeerDisconnectedEventListener {
    void handlePeerDisconnected(PeerId peerId);
}
