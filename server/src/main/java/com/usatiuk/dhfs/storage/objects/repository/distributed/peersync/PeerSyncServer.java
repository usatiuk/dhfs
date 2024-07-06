package com.usatiuk.dhfs.storage.objects.repository.distributed.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.DhfsObjectPeerSyncGrpc;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.GetSelfInfoRequest;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.SyncPeersData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.CertificateTools;
import com.usatiuk.dhfs.storage.objects.repository.distributed.HostInfo;
import com.usatiuk.dhfs.storage.objects.repository.distributed.PersistentRemoteHostsService;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.util.UUID;

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
        try {
            builder.addMyPeers(PeerInfo.newBuilder()
                    .setUuid(persistentRemoteHostsService.getSelfUuid().toString())
                    .setCert(ByteString.copyFrom(persistentRemoteHostsService.getSelfCertificate().getEncoded()))
                    .build());
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }

        for (var np : request.getMyPeersList()) {
            try {
                persistentRemoteHostsService.addHost(
                        new HostInfo(UUID.fromString(np.getUuid()),
                                CertificateTools.certFromBytes(np.getCert().toByteArray())));
            } catch (CertificateException e) {
                Log.error("Error adding peer " + np.getUuid(), e);
            }
        }

        return Uni.createFrom().item(builder.build());
    }

    @Override
    @Blocking
    public Uni<PeerInfo> getSelfInfo(GetSelfInfoRequest request) {
        try {
            return Uni.createFrom().item(
                    PeerInfo.newBuilder()
                            .setUuid(persistentRemoteHostsService.getSelfUuid().toString())
                            .setCert(ByteString.copyFrom(persistentRemoteHostsService.getSelfCertificate().getEncoded()))
                            .build());
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
