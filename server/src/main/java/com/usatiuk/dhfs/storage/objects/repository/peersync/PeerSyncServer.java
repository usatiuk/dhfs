package com.usatiuk.dhfs.storage.objects.repository.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.peersync.DhfsObjectPeerSyncGrpc;
import com.usatiuk.dhfs.objects.repository.peersync.GetSelfInfoRequest;
import com.usatiuk.dhfs.objects.repository.peersync.PeerInfo;
import com.usatiuk.dhfs.storage.objects.repository.PersistentRemoteHostsService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.security.cert.CertificateEncodingException;

@GrpcService
public class PeerSyncServer implements DhfsObjectPeerSyncGrpc {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

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
