package com.usatiuk.dhfs.storage.objects.repository.distributed.peersync;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.PeerInfo;
import com.usatiuk.dhfs.objects.repository.distributed.peersync.SyncPeersData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
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
            try {
                builder.addMyPeers(PeerInfo.newBuilder().setUuid(persistentRemoteHostsService.getSelfUuid().toString())
                        .setCert(ByteString.copyFrom(persistentRemoteHostsService.getSelfCertificate().getEncoded()))
                        .build());
            } catch (CertificateEncodingException e) {
                throw new RuntimeException(e);
            }
            return client.syncPeers(builder.build());
        });

        for (var np : ret.getMyPeersList()) {
            try {
                persistentRemoteHostsService.addHost(
                        new HostInfo(UUID.fromString(np.getUuid()),
                                CertificateTools.certFromBytes(np.getCert().toByteArray())));
            } catch (CertificateException e) {
                Log.error("Error adding peer " + np.getUuid(), e);
            }
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
