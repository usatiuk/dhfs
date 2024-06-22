package com.usatiuk.dhfs.storage.objects.repository.distributed.peersync;

import com.usatiuk.dhfs.objects.repository.distributed.peersync.DhfsObjectPeerSyncGrpc;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.SyncPeersData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.HostInfo;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@GrpcService
public class PeerSyncServer implements DhfsObjectPeerSyncGrpc {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @Override
    @Blocking
    public Uni<SyncPeersData> syncPeers(SyncPeersData request) {
        var builder = SyncPeersData.newBuilder();
        builder.setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());
        for (var h : persistentRemoteHostsService.getHosts()) {
            builder.addMyPeers(h.toPeerInfo());
        }
        builder.addMyPeers(PeerInfo.newBuilder().setUuid(persistentRemoteHostsService.getSelfUuid().toString()).build());

        for (var np : request.getMyPeersList()) {
            persistentRemoteHostsService.addHost(new HostInfo(np.getUuid()));
        }

        return Uni.createFrom().item(builder.build());
    }
}
