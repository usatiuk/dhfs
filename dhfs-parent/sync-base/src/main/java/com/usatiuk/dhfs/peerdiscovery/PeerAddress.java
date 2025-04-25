package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PeerId;

import java.io.Serializable;

public interface PeerAddress extends Serializable {
    PeerId peer();

    PeerAddressType type();
}
