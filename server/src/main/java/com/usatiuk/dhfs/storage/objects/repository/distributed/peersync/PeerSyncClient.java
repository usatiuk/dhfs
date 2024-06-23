package com.usatiuk.dhfs.storage.objects.repository.distributed.peersync;

import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.SyncPeersData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.HostInfo;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteHostManager;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RpcClientFactory;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class PeerSyncClient {
    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RpcClientFactory rpcClientFactory;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    public void syncPeersOne(UUID host) {
        var ret = rpcClientFactory.withPeerSyncClient(host, client -> {
            var builder = SyncPeersData.newBuilder();
            builder.setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());
            for (var h : persistentRemoteHostsService.getHosts()) {
                builder.addMyPeers(h.toPeerInfo());
            }
            builder.addMyPeers(PeerInfo.newBuilder().setUuid(persistentRemoteHostsService.getSelfUuid().toString()).build());
            return client.syncPeers(builder.build());
        });

        for (var np : ret.getMyPeersList()) {
            persistentRemoteHostsService.addHost(new HostInfo(np.getUuid()));
        }
    }

    public void syncPeersAll() {
        for (var h : remoteHostManager.getSeenHosts()) {
            try {
                syncPeersOne(h);
            } catch (Exception e) {
                Log.info("Failed syncing hosts with " + h, e);
            }
        }
    }
}
