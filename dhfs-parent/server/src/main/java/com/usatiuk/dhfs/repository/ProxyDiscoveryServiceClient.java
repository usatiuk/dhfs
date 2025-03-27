package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Collection;

@ApplicationScoped
public class ProxyDiscoveryServiceClient {
    @Inject
    RpcClientFactory rpcClientFactory;

    public Collection<PeerId> getAvailablePeers(PeerId peerId) {
        return rpcClientFactory.withObjSyncClient(peerId, (peer, client) -> {
            var reply = client.proxyAvailableGet(ProxyAvailableRequest.getDefaultInstance());
            return reply.getAvailableTargetsList().stream().map(ProxyAvailableInfo::getUuid).map(PeerId::of).toList();
        });
    }

}
