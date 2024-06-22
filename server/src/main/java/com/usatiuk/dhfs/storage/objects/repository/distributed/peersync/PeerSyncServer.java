package com.usatiuk.dhfs.storage.objects.repository.distributed.peersync;

import com.usatiuk.dhfs.objects.repository.distributed.peersync.DhfsObjectPeerSyncGrpc;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.SyncPeersData;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

@GrpcService
public class PeerSyncServer implements DhfsObjectPeerSyncGrpc {

    @Override
    public Uni<SyncPeersData> syncPeers(SyncPeersData request) {
        return null;
    }
}
