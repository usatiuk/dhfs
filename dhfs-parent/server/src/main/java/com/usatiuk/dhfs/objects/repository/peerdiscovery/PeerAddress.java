package com.usatiuk.dhfs.objects.repository.peerdiscovery;

import com.usatiuk.dhfs.objects.PeerId;

public interface PeerAddress {
    PeerId peer();

    PeerAddressType type();
}
