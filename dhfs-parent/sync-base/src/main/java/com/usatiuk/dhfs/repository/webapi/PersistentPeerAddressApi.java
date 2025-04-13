package com.usatiuk.dhfs.repository.webapi;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.PeerManager;
import com.usatiuk.dhfs.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.repository.peerdiscovery.PeerAddrStringHelper;
import com.usatiuk.dhfs.repository.peersync.PeerInfoService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;

import java.util.Collection;

@Path("/peers-addr-manage")
public class PersistentPeerAddressApi {
    @Inject
    PeerInfoService peerInfoService;
    @Inject
    PeerManager peerManager;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @Path("{peerId}")
    @PUT
    public void addPeerAddress(String peerAddr, @PathParam("peerId") String peerId) {
        if (peerAddr.isEmpty()) {
            deletePeerAddress(peerId);
            return;
        }
        persistentPeerDataService.addPersistentPeerAddress(PeerId.of(peerId), PeerAddrStringHelper.parseNoPeer(PeerId.of(peerId), peerAddr).orElseThrow(IllegalArgumentException::new));
    }

    @Path("{peerId}")
    @DELETE
    public void deletePeerAddress(@PathParam("peerId") String peerId) {
        persistentPeerDataService.removePersistentPeerAddress(PeerId.of(peerId));
    }

    @Path("{peerId}")
    @GET
    public String getPeerAddress(@PathParam("peerId") String peerId) {
        return persistentPeerDataService.getPersistentPeerAddress(PeerId.of(peerId)).toString();
    }

    @Path("")
    @GET
    public Collection<PeerAddressInfo> getPeerAddresses() {
        return persistentPeerDataService.getPersistentPeerAddresses()
                .stream()
                .map(p -> new PeerAddressInfo(p.peer().toString(), p.address().getHostAddress() + ":" + p.port() + ":" + p.securePort()))
                .toList();
    }
}
