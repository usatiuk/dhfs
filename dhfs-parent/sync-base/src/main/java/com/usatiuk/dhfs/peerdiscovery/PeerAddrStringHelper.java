package com.usatiuk.dhfs.peerdiscovery;

import com.usatiuk.dhfs.peersync.PeerId;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/**
 * Helper class for parsing peer addresses from strings.
 * <p>
 * The expected format is: <peerId>:<ip>:<port>:<securePort>
 * </p>
 */
public class PeerAddrStringHelper {

    /**
     * Parses a string into an IpPeerAddress object.
     *
     * @param addr the string to parse
     * @return an Optional containing the parsed IpPeerAddress, or an empty Optional if the string is empty
     */
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

    /**
     * Parses a string into an IpPeerAddress object, with a manually provided peer ID.
     *
     * @param peerId the peer ID to use
     * @param addr   the string to parse
     * @return an Optional containing the parsed IpPeerAddress, or an empty Optional if the string is empty
     */
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
