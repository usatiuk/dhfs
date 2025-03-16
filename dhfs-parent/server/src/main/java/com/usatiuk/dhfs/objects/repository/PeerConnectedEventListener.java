package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.PeerId;

public interface PeerConnectedEventListener {
    void handlePeerConnected(PeerId peerId);
}
