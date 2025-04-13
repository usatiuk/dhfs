package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.PeerId;

import java.io.Serializable;

public interface PeerAddress extends Serializable {
    PeerId peer();

    PeerAddressType type();
}
