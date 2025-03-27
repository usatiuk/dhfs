package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.PeerId;

public record ProxyPeerAddress(PeerId peer, PeerAddressType type,
                               PeerId proxyThrough) implements PeerAddress {
}
