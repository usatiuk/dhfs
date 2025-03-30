package com.usatiuk.dhfs.repository.peerdiscovery;

import com.usatiuk.dhfs.PeerId;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

public class PeerAddrStringHelper {

    public static Optional<IpPeerAddress> parse(String addr) {
        if (addr.isEmpty()) {
            return Optional.empty();
        }
        var split = addr.split(":");
        try {
            return Optional.of(new IpPeerAddress(PeerId.of(split[0]), PeerAddressType.LAN, InetAddress.getByName(split[1]),
                    Integer.parseInt(split[2]), Integer.parseInt(split[3])));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Optional<IpPeerAddress> parseNoPeer(PeerId peerId, String addr) {
        if (addr.isEmpty()) {
            return Optional.empty();
        }
        var split = addr.split(":");
        try {
            return Optional.of(new IpPeerAddress(peerId, PeerAddressType.LAN, InetAddress.getByName(split[0]),
                    Integer.parseInt(split[1]), Integer.parseInt(split[2])));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
}
