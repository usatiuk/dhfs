package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.InitialSyncProcessor;
import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.peersync.PeerInfoService;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RemoteObjectInitialSyncProcessor implements InitialSyncProcessor<RemoteObjectMeta> {
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    Transaction curTx;
    @Inject
    PeerInfoService peerInfoService;

    @Override
    public void prepareForInitialSync(PeerId from, JObjectKey key) {
    }

    @Override
    public void handleCrash(JObjectKey key) {
        var data = curTx.get(RemoteObjectMeta.class, key).orElseThrow();
        var versionSum = data.versionSum();
        for (var p : peerInfoService.getPeersNoSelf()) {
            if (data.knownRemoteVersions().getOrDefault(p.id(), 0L) != versionSum) {
                invalidationQueueService.pushInvalidationToOne(p.id(), key);
                Log.infov("Pushing after crash invalidation to {0} for {1}", p.id(), key);
            }
        }
    }
}
