package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PeerId;

import java.net.InetAddress;

public record IpPeerAddress(PeerId peer, PeerAddressType type,
                            InetAddress address, int port, int securePort) implements PeerAddress {
    public IpPeerAddress withType(PeerAddressType type) {
        return new IpPeerAddress(peer, type, address, port, securePort);
    }
}
