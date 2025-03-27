package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.PeerId;

public interface PeerAddress {
    PeerId peer();

    PeerAddressType type();
}
