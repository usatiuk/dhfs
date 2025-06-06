package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PeerId;

import java.net.InetAddress;

/**
 * Represents a peer address with an IP address and port.
 */
public record IpPeerAddress(PeerId peer, PeerAddressType type,
                            InetAddress address, int port, int securePort) implements PeerAddress {
    public IpPeerAddress withType(PeerAddressType type) {
        return new IpPeerAddress(peer, type, address, port, securePort);
    }

    @Override
    public String toString() {
        return "IP: " + address.getHostAddress() +
                ":" + port +
                ":" + securePort +
                ", type: " + type;
    }
}
