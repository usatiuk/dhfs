package com.usatiuk.dhfs.objects.repository.peerdiscovery;

import com.usatiuk.dhfs.objects.PeerId;

import java.net.InetAddress;

public record IpPeerAddress(PeerId peer, PeerAddressType type,
                            InetAddress address, int port, int securePort) implements PeerAddress {
}
