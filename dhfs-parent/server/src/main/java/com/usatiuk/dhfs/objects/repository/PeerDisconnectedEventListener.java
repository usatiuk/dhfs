package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.PeerId;

public interface PeerDisconnectedEventListener {
    void handlePeerDisconnected(PeerId peerId);
}
